--Begin Revision History
--<<<
-- 08-Mar-2017 05:37:56 C9988598 /main/11
-- 
--<<<
-- 13-Jun-2017 07:27:11 C9919246 /main/12
-- 
-- <<<
--End Revision History  
CREATE OR REPLACE PACKAGE BODY xxar_invoices_adjust_pkg IS

   ---------------------------------------------------------------------------------------------------------
   --    Owner        : EATON CORPORATION.
   --    Application  : Account Receivables
   --    Schema       : APPS
   --    Compile AS   : APPS
   --    File Name    : XXAR_INVOICES_ADJ_PKG.pkb
   --    Date         : 18-Feb-2014
   --    Author       : Seema Machado
   --    Description  : Package Body for AR Invoices Conversion Adjustment creation
   --    Version      : $ETNHeader: /CCSTORE/ccweb/C9919246/C9919246_XXAR_TOP_DEV/vobs/AR_TOP/xxar/12.0.0/install/xxar_invoices_adj_pkg.pkb /main/12 13-Jun-2017 07:27:11 C9919246  $
   --    Parameters  :
   --    pin_batch_id     : List all unique batches from staging table , this will
   --                         be NULL for first Conversion Run.
   --    piv_ou          : R12 Operating Unit. If provided then program will
   --                   run only for specified Operating unit
   --    piv_rec_trx_act     : Adjustment Type
   --    piv_adj_date      : Adjustment date
   --
   --    Change History
   --  =====================================================================================================
   --    v1.0      Seema Machado     15-Jan-2014  Initial Creation
   --    v1.1      Seema Machado     01-Apr-2015  Changes done to accommodate the tax CR
   --    v2.0      Piyush Ojha       26-Feb-2016  CR361880 Raised for error You cannon overapply this transaction.
   --                                             Creating two adjustments if amount to adjust is greater than total line amount.
   --                                             Adding PO where clause for Defect 5482 duplicate invoices adjustment
   --    v3.0      Seema Machado     18-Mar-2016  Changes done to accommodate CR371665
   --                                       3.1:  For incorrect adjustments created when there are multiple installments (Defect 5520)
   --                                       3.2:  For incorrect adjustments for duplicate invoice numbers.
   --                                       3.3:  For incorrect adjustments created when multiple installments are partially applied
   --    v4.0      Bhaskar Pedipina  10-Jan-2017  Mock5 Defect# 12837, Multiple Installments logic change to adjust LINE and TAX amount seperately
   --    v5.0      Preeti Sethi      07-Mar-2017  Defect#15485 : Code Change done to fix the amount remaining for each installment efrom 11i to R12 specially for payment term ESPECIAL 02 to 07.
   --    v6.0      Akshay Raikar     12-Jun-2017  Changes done for Defect# 17766, Performance Issue of AR Adjustment Program. Added org_id join between ra_customer_trx_all and ra_customer_trx_lines_all
   --  =====================================================================================================
   -- Global variable declarations
   g_batch_id NUMBER;

   g_debug_err VARCHAR2(2000);

   g_org_id  NUMBER; --    := fnd_profile.VALUE ('ORG_ID');
   g_user_id NUMBER := fnd_global.user_id;

   g_err_lmt NUMBER := fnd_profile.VALUE('ETN_FND_ERROR_TAB_LIMIT');

   g_err_indx NUMBER := 0;

   g_error_tab xxetn_common_error_pkg.g_source_tab_type;

   g_request_id fnd_concurrent_requests.request_id%TYPE := fnd_global.conc_request_id;

   g_batch_source ar_batch_sources_all.NAME%TYPE := 'CONVERSION';

   g_created_from ar_adjustments_all.created_from%TYPE := 'CONVERSION';

   g_sysdate CONSTANT DATE := SYSDATE;

   --
   -- ========================
   -- Procedure: print_log_message
   -- =============================================================================
   --   This procedure is used to write message to log file.
   -- =============================================================================
   PROCEDURE PRINT_LOG_MESSAGE(piv_message IN VARCHAR2) IS
   BEGIN
      IF NVL(g_request_id, 0) > 0
      THEN
         fnd_file.put_line(fnd_file.LOG, piv_message);
      END IF;
   EXCEPTION
      WHEN OTHERS THEN
         NULL;
   END PRINT_LOG_MESSAGE;

   --
   -- ========================
   -- Procedure: update_for_correction
   -- =============================================================================
   --   Procedure to update for Adjustment corrections
   -- =============================================================================
   PROCEDURE UPDATE_FOR_CORRECTION(g_org_id IN NUMBER) IS
   BEGIN
   --ver3.1 changes start
   /*
      UPDATE xxar_invoices_stg xis
         SET process_flag = 'W'
             , request_id = g_request_id
       , last_update_date = sysdate
       --WHERE process_flag = 'C'
     WHERE process_flag IN ('C', 'W')
         AND batch_id = g_batch_id
         AND xis.org_id = NVL (g_org_id, xis.org_id)
         AND EXISTS (
                SELECT 1
                  FROM ra_customer_trx_all rct,
                       ra_customer_trx_lines_all rctl,
                       ar_payment_schedules_all aps
                 WHERE rct.customer_trx_id = rctl.customer_trx_id
                   AND rct.customer_trx_id = aps.customer_trx_id
                   AND rct.org_id = aps.org_id
                   AND aps.amount_due_remaining <>
                                              xis.leg_inv_amount_due_remaining
                   AND rct.trx_number = xis.leg_trx_number
                   AND rct.org_id = xis.org_id
                   AND rct.cust_trx_type_id = xis.transaction_type_id);


        */

     UPDATE xxar_invoices_stg xis
       SET process_flag     = 'W'
        ,request_id       = g_request_id
        ,last_update_date = SYSDATE
      --WHERE process_flag = 'C'
       WHERE process_flag IN ('C', 'W')
       AND batch_id = g_batch_id
       AND xis.org_id = NVL(g_org_id, xis.org_id)
       AND EXISTS
       (SELECT 1
          FROM ra_customer_trx_all       rct
            ,ra_customer_trx_lines_all rctl
            ,ar_payment_schedules_all  aps
            ,(SELECT customer_trx_id
                 ,SUM(amount_due_remaining) amount_due_remaining
               FROM ar_payment_schedules_all
              GROUP BY customer_trx_id) aps1
            ,xxar_inv_pmt_schedule_stg xps
           WHERE rct.customer_trx_id = rctl.customer_trx_id
           AND rct.org_id = rctl.org_id                         --added for v6.0 defect# 17766
           AND rct.customer_trx_id = aps.customer_trx_id
           AND rct.org_id = aps.org_id
           AND rct.trx_number = xis.leg_trx_number
           AND rct.org_id = xis.org_id
           AND rct.cust_trx_type_id = xis.transaction_type_id
           --ver3.2 changes start
           AND xis.interface_line_attribute15 =
             rctl.interface_line_attribute15
          --ver3.2 changes end
           AND aps1.customer_trx_id = aps.customer_trx_id

          --v5.0 commented for Defect#15485
           /* AND aps1.amount_due_remaining -
              NVL(xis.leg_inv_amount_due_remaining, 0) <> 0
            AND aps1.amount_due_remaining <>
              NVL(xis.leg_inv_amount_due_remaining, 0)
           */
           --v5.0 code change ends for Defect#15485 ---
            AND aps.amount_due_remaining -
              NVL(xps.leg_amount_due_remaining, 0) <> 0
            AND aps1.amount_due_remaining <>
              NVL(xps.leg_amount_due_remaining, 0)
            AND xps.leg_customer_trx_id = xis.leg_customer_trx_id
            AND aps.terms_sequence_number =
              xps.leg_terms_sequence_number
            AND xis.batch_id = xps.batch_id
            AND xis.leg_source_system = xps.leg_source_system
          );

    --ver3.1 changes end

      COMMIT;
   EXCEPTION
      WHEN OTHERS THEN
         NULL;
   END UPDATE_FOR_CORRECTION;

   --
   -- ========================
   -- Procedure: log_errors
   -- =============================================================================
   --   This procedure will log the errors in the error report using error
   --   framework
   -- =============================================================================
   PROCEDURE LOG_ERRORS(pin_transaction_id      IN NUMBER DEFAULT NULL
                       ,piv_source_keyname1     IN xxetn_common_error.source_keyname1%TYPE DEFAULT NULL
                       ,piv_source_keyvalue1    IN xxetn_common_error.source_keyvalue1%TYPE DEFAULT NULL
                       ,piv_source_keyname2     IN xxetn_common_error.source_keyname2%TYPE DEFAULT NULL
                       ,piv_source_keyvalue2    IN xxetn_common_error.source_keyvalue2%TYPE DEFAULT NULL
                       ,piv_source_keyname3     IN xxetn_common_error.source_keyname3%TYPE DEFAULT NULL
                       ,piv_source_keyvalue3    IN xxetn_common_error.source_keyvalue3%TYPE DEFAULT NULL
                       ,piv_source_keyname4     IN xxetn_common_error.source_keyname4%TYPE DEFAULT NULL
                       ,piv_source_keyvalue4    IN xxetn_common_error.source_keyvalue4%TYPE DEFAULT NULL
                       ,piv_source_keyname5     IN xxetn_common_error.source_keyname5%TYPE DEFAULT NULL
                       ,piv_source_keyvalue5    IN xxetn_common_error.source_keyvalue5%TYPE DEFAULT NULL
                       ,piv_source_column_name  IN xxetn_common_error.source_column_name%TYPE DEFAULT NULL
                       ,piv_source_column_value IN xxetn_common_error.source_column_value%TYPE DEFAULT NULL
                       ,piv_source_table        IN xxetn_common_error.source_table%TYPE DEFAULT NULL
                       ,piv_error_type          IN xxetn_common_error.ERROR_TYPE%TYPE
                       ,piv_error_code          IN xxetn_common_error.ERROR_CODE%TYPE
                       ,piv_error_message       IN xxetn_common_error.error_message%TYPE
                       ,pov_return_status       OUT VARCHAR2
                       ,pov_error_msg           OUT VARCHAR2) IS
      l_return_status VARCHAR2(1);
      l_error_message VARCHAR2(2000);
   BEGIN
      --  xxetn_debug_pkg.add_debug ( p_err_msg );
      g_err_indx := g_err_indx + 1;
      g_error_tab(g_err_indx).source_table := NVL(piv_source_table,
                                                  'XXAR_INVOICES_STG');
      g_error_tab(g_err_indx).interface_staging_id := pin_transaction_id;
      g_error_tab(g_err_indx).source_keyname1 := piv_source_keyname1;
      g_error_tab(g_err_indx).source_keyvalue1 := piv_source_keyvalue1;
      g_error_tab(g_err_indx).source_keyname2 := piv_source_keyname2;
      g_error_tab(g_err_indx).source_keyvalue2 := piv_source_keyvalue2;
      g_error_tab(g_err_indx).source_keyname3 := piv_source_keyname3;
      g_error_tab(g_err_indx).source_keyvalue3 := piv_source_keyvalue3;
      g_error_tab(g_err_indx).source_keyname4 := piv_source_keyname4;
      g_error_tab(g_err_indx).source_keyvalue4 := piv_source_keyvalue4;
      g_error_tab(g_err_indx).source_keyname5 := piv_source_keyname5;
      g_error_tab(g_err_indx).source_keyvalue5 := piv_source_keyvalue5;
      g_error_tab(g_err_indx).source_column_name := piv_source_column_name;
      g_error_tab(g_err_indx).source_column_value := piv_source_column_value;
      g_error_tab(g_err_indx).ERROR_TYPE := piv_error_type;
      g_error_tab(g_err_indx).ERROR_CODE := piv_error_code;
      g_error_tab(g_err_indx).error_message := piv_error_message;
      IF MOD(g_err_indx, g_err_lmt) = 0
      THEN
         xxetn_common_error_pkg.add_error(pov_return_status => l_return_status,
                                          pov_error_msg => l_error_message,
                                          pi_source_tab => g_error_tab,
                                          pin_batch_id => g_batch_id);
         g_error_tab.DELETE;
         g_err_indx        := 0;
         pov_return_status := l_return_status;
         pov_error_msg     := l_error_message;
      END IF;
      print_log_message('p_err_msg:' || piv_error_message);
   EXCEPTION
      WHEN OTHERS THEN
         xxetn_debug_pkg.add_debug('Error: Exception occured in log_errors procedure ' ||
                                   SUBSTR(SQLERRM, 1, 150));
   END LOG_ERRORS;

   --
   -- ========================
   -- Procedure: main
   -- =============================================================================
   --   Main Procedure to be called by concurrent program
   -- =============================================================================
   PROCEDURE MAIN(pov_errbuf      OUT VARCHAR2
                 ,pon_retcode     OUT NUMBER
                 ,piv_dummy1      IN VARCHAR2
                 ,pin_batch_id    IN NUMBER
                 ,piv_ou          IN VARCHAR2
                 ,piv_rec_trx_act IN VARCHAR2
                 ,piv_adj_date    IN VARCHAR2) IS
      CURSOR cur_trx IS
      --ver3.0 changes start
      /*
                        SELECT aps.amount_due_remaining -
                               NVL(xis.leg_inv_amount_due_remaining, 0) amount_to_adjust
                              ,SUM(xis.leg_line_amount) trx_total
                              ,rct.customer_trx_id
                              ,aps.payment_schedule_id
                              ,rct.set_of_books_id
                              ,rct.trx_number
                              ,rct.org_id
                              ,xis.leg_customer_trx_id
                              ,aps.amount_due_remaining
                              ,xis.leg_inv_amount_due_remaining
                              ,'LINE' TYPE
                              ,xis.leg_currency_code
                          FROM xxar_invoices_stg        xis
                              ,ra_customer_trx_all      rct
                              ,ar_payment_schedules_all aps
                              ,
                               --     ra_batch_sources_all rbs,
                               ra_cust_trx_types_all rctt
                              ,
                         WHERE xis.process_flag = 'W'
                           AND xis.batch_id = g_batch_id
                           AND rct.org_id = NVL(g_org_id, rct.org_id)
                           AND rct.org_id = rctt.org_id
                           AND rct.cust_trx_type_id = rctt.cust_trx_type_id
                           AND rctt.TYPE = xis.trx_type
                           AND nvl(rct.purchase_order, 1) =
                               nvl(xis.LEG_PURCHASE_ORDER, 1) --v2.0
                              --v1.1 starts
                              --   AND rbs.NAME = g_batch_source
                              --   AND rct.batch_source_id = rbs.batch_source_id
                              --   AND rct.org_id = rbs.org_id
                              --v1.1 ends
                           AND xis.leg_trx_number = rct.trx_number
                           AND xis.org_id = rct.org_id
                           AND rct.customer_trx_id = aps.customer_trx_id
                           AND rct.org_id = aps.org_id
                              --AND adj.trx_total <> 0
                           AND aps.amount_due_remaining -
                               NVL(xis.leg_inv_amount_due_remaining, 0) <> 0
                           AND aps.amount_due_remaining <>
                               NVL(xis.leg_inv_amount_due_remaining, 0)
                         GROUP BY rct.customer_trx_id
                                 ,xis.leg_inv_amount_due_remaining
                                 ,aps.payment_schedule_id
                                 ,rct.set_of_books_id
                                 ,rct.trx_number
                                 ,rct.org_id
                                 ,xis.leg_customer_trx_id
                                 ,aps.amount_due_remaining
                                 ,TYPE
                                 ,xis.leg_currency_code;

                        */
         SELECT amount_to_adjust
               ,trx_total
               ,customer_trx_id
               ,payment_schedule_id
               ,set_of_books_id
               ,trx_number
               ,org_id
               ,leg_customer_trx_id
               ,amount_due_remaining
               ,leg_amount_due_remaining
               ,TYPE
               ,leg_currency_code
           FROM (SELECT aps.amount_due_remaining -
                        NVL(xps.leg_amount_due_remaining, 0) --ver3.3
                        amount_to_adjust
                       ,SUM(xis.leg_line_amount) trx_total
                       ,rct.customer_trx_id
                       ,aps.payment_schedule_id
                       ,rct.set_of_books_id
                       ,rct.trx_number
                       ,rct.org_id
                       ,xis.leg_customer_trx_id
                       ,aps.amount_due_remaining
                       ,xps.leg_amount_due_remaining
                       ,'LINE' TYPE
                       ,xis.leg_currency_code
                       ,xis.leg_source_system --ver3.1
                   FROM xxar_invoices_stg        xis
                       ,ra_customer_trx_all      rct
                       ,ar_payment_schedules_all aps
                        --ver3.1 changes start
                       ,xxar_inv_pmt_schedule_stg xps
                       ,(SELECT customer_trx_id
                               ,SUM(amount_due_remaining) amount_due_remaining
                           FROM ar_payment_schedules_all
                          GROUP BY customer_trx_id) aps1
                        --ver3.1 changes end
                       ,ra_cust_trx_types_all     rctt
                       ,ra_customer_trx_lines_all rctl --ver3.2
                  WHERE xis.process_flag = 'W'
                    AND xis.batch_id = g_batch_id
                       --ver3.1 changes start
                    AND aps1.customer_trx_id = aps.customer_trx_id
                       --ver3.1 changes end
                       --ver3.2 changes start
                    AND rct.customer_trx_id = rctl.customer_trx_id
                       --ver3.2 changes end
                    AND rct.org_id = NVL(g_org_id, rct.org_id)
                    AND rct.org_id = rctt.org_id
                    AND rct.cust_trx_type_id = rctt.cust_trx_type_id
                    AND rctt.TYPE = xis.trx_type
                       --ver3.2 changes start
                       --AND nvl(rct.purchase_order,1) = nvl(xis.LEG_PURCHASE_ORDER,1)  --v2.0
                    AND xis.interface_line_attribute15 =
                        rctl.interface_line_attribute15
                       --ver3.2 changes end
                    AND xis.leg_trx_number = rct.trx_number
                    AND xis.org_id = rct.org_id
                    AND rct.customer_trx_id = aps.customer_trx_id
                    AND rct.org_id = aps.org_id
                       --ver3.1 changes start
                       --AND aps.amount_due_remaining - NVL (xis.leg_inv_amount_due_remaining, 0) <> 0
                       --AND aps.amount_due_remaining <> NVL (xis.leg_inv_amount_due_remaining, 0)
                 --v5.0 commented for Defect#15485
                    /*AND aps1.amount_due_remaining -
                        NVL(xis.leg_inv_amount_due_remaining, 0) <> 0
                    AND aps1.amount_due_remaining <>
                        NVL(xis.leg_inv_amount_due_remaining, 0)
                    */
                    -- v5.0 code change ends--
                     --ver3.1 changes end
                       --ver3.3 changes start
                    AND aps.amount_due_remaining -
                        NVL(xps.leg_amount_due_remaining, 0) <> 0
                    AND aps1.amount_due_remaining <>
                        NVL(xps.leg_amount_due_remaining, 0)
                    AND xps.leg_customer_trx_id = xis.leg_customer_trx_id
                    AND aps.terms_sequence_number =
                        xps.leg_terms_sequence_number
                    AND xis.batch_id = xps.batch_id
                    AND xis.leg_source_system = xps.leg_source_system
                 --ver3.3 changes start
                  GROUP BY rct.customer_trx_id
                           --ver3.3 changes start
                           --xis.leg_inv_amount_due_remaining,
                          ,xps.leg_amount_due_remaining
                           --ver3.3 changes end
                          ,aps.payment_schedule_id
                          ,rct.set_of_books_id
                          ,rct.trx_number
                          ,rct.org_id
                          ,xis.leg_customer_trx_id
                          ,aps.amount_due_remaining
                          ,TYPE
                          ,xis.leg_currency_code
                          ,xis.leg_source_system) --ver3.1
          ORDER BY leg_source_system
                  ,leg_customer_trx_id;
      --ver3.0 changes end
      l_line_status         VARCHAR2(1) := NULL;
      l_adj_status          VARCHAR2(1) := NULL;
      l_message             VARCHAR2(4000) := NULL;
      l_adj_rec             ar_adjustments%ROWTYPE;
      l_api_name            VARCHAR2(20);
      l_api_version         NUMBER;
      l_called_from         VARCHAR2(10);
      l_check_amount        VARCHAR2(1);
      l_chk_approval_limits VARCHAR2(1);
      l_commit_flag         VARCHAR2(1);
      l_init_msg_list       VARCHAR2(1);
      l_move_deferred_tax   VARCHAR2(10);
      l_msg_count           NUMBER;
      l_msg_data            VARCHAR2(2000);
      l_new_adjust_id       ar_adjustments.adjustment_id%TYPE;
      l_new_adjust_number   ar_adjustments.adjustment_number%TYPE;
      l_old_adjust_id       ar_adjustments.adjustment_id%TYPE;
      l_return_status       VARCHAR2(5);
      l_validation_level    NUMBER;
      l_total_count         NUMBER := 0;
      l_success_count       NUMBER := 0;
      l_failed_count        NUMBER := 0;
      l_ccid                NUMBER := NULL;
      l_precision           NUMBER := 2;
      l_err_excep EXCEPTION;
      l_log_ret_status      VARCHAR2(50);
      l_log_err_msg         VARCHAR2(2000);
      l_total_line_amount   NUMBER; --added for v2.0
      l_leg_customer_trx_id NUMBER := -1; --ver3.1
      l_trx_adj_status      VARCHAR2(1) := 'C'; --ver3.1  --this status is added to ensure transaction is marked C only when all installments are created successfully
   BEGIN
      pon_retcode := 0;
      xxetn_debug_pkg.initialize_debug(pov_err_msg => g_debug_err,
                                       piv_program_name => 'ETN_AR_INVOICE_CONVERSION');
      xxetn_debug_pkg.add_debug('Program Parameters');
      xxetn_debug_pkg.add_debug('---------------------------------------------');
      xxetn_debug_pkg.add_debug('Batch ID            : ' || pin_batch_id);
      xxetn_debug_pkg.add_debug('Operating Unit : ' || piv_ou);
      xxetn_debug_pkg.add_debug('Receivable Transaction     : ' ||
                                piv_rec_trx_act);
      xxetn_debug_pkg.add_debug('Adjustment Date    : ' || piv_adj_date);
      g_batch_id := pin_batch_id;
      BEGIN
         IF piv_ou IS NOT NULL
         THEN
            SELECT hou.organization_id
              INTO g_org_id
              FROM apps.hr_operating_units hou
             WHERE UPPER(hou.NAME) = UPPER(piv_ou)
               AND TRUNC(NVL(hou.date_to, g_sysdate)) >= TRUNC(g_sysdate);
         END IF;
      EXCEPTION
         WHEN OTHERS THEN
            print_log_message('Error while deriving org_ud for parameter provided');
            xxetn_debug_pkg.add_debug('Error while deriving org_ud for parameter provided');
            RAISE l_err_excep;
      END;
      -- Mark records to be considered for adjustment creation
      xxetn_debug_pkg.add_debug('Updating records to be picked for adjustment');
      update_for_correction(g_org_id);
      -- Open cursor for adjustment creation where amount_due_orignal <> amount_due_remaining in staging table
      FOR rec_trx IN cur_trx
      LOOP
         --start of V2.0
         l_total_line_amount := NULL;
         --getting total line amount
        /* BEGIN
            SELECT SUM(EXTENDED_AMOUNT)
              INTO l_total_line_amount
              FROM ra_customer_trx_lines_all
             WHERE customer_trx_id = rec_trx.customer_trx_id
               AND org_id = rec_trx.org_id
               AND line_type = 'LINE';
         EXCEPTION
            WHEN OTHERS THEN
               l_total_line_amount := NULL;
         END;
         --end of v2.0  */                                 -- Commented for v4.0

     -- Start of v4.0 Changes
     BEGIN
        SELECT SUM(amount_line_items_remaining)
              INTO l_total_line_amount
              FROM ar_payment_schedules_all
             WHERE customer_trx_id = rec_trx.customer_trx_id
               AND org_id = rec_trx.org_id
               AND payment_schedule_id = rec_trx.payment_schedule_id ;
         EXCEPTION
            WHEN OTHERS THEN
               l_total_line_amount := NULL;
         END;
     -- End of v4.0 Changes

         IF abs(rec_trx.amount_to_adjust) >
            abs(nvl(l_total_line_amount, 0))
         THEN
            --case where need to create 2 adjustment lines
            FOR line_cur IN (SELECT line_type
                                   ,SUM(extended_amount) amount
                               FROM ra_customer_trx_lines_all
                              WHERE customer_trx_id =
                                    rec_trx.customer_trx_id
                                AND org_id = rec_trx.org_id
                                AND line_type IN ('LINE', 'TAX')
                              GROUP BY line_type)
            LOOP
               l_line_status := 'S';
               --ver3.1 changes start
               --If for an invoice with installements, invoice must be marked W even if one installment fails
               IF l_leg_customer_trx_id <> rec_trx.leg_customer_trx_id
               THEN
                  l_trx_adj_status      := 'C';
                  l_leg_customer_trx_id := rec_trx.leg_customer_trx_id;
               END IF;
               --ver3.1 changes end
               l_adj_status  := 'C';
               l_message     := NULL;
               l_ccid        := NULL;
               l_total_count := l_total_count + 1;
               mo_global.init('AR');
               mo_global.set_org_context(rec_trx.org_id, '', 'S');
               l_adj_rec             := NULL;
               l_api_name            := NULL;
               l_api_version         := 1.0;
               l_called_from         := NULL;
               l_check_amount        := NULL;
               l_chk_approval_limits := 'F';
               l_commit_flag         := NULL;
               l_init_msg_list       := fnd_api.g_true;
               l_move_deferred_tax   := 'Y';
               l_msg_count           := 0;
               l_msg_data            := NULL;
               l_new_adjust_id       := NULL;
               l_new_adjust_number   := NULL;
               l_old_adjust_id       := NULL;
               l_return_status       := NULL;
               l_validation_level    := fnd_api.g_valid_level_full;
               BEGIN
                  SELECT PRECISION
                    INTO l_precision
                    FROM fnd_currencies fc
                   WHERE currency_code = rec_trx.leg_currency_code
                     AND ROWNUM = 1;
               EXCEPTION
                  WHEN OTHERS THEN
                     l_precision := 2;
               END;
               /* l_adj_rec.acctd_amount :=
               (-1) * ROUND (rec_trx.amount_to_adjust, l_precision);*/ --commented for v2.0
               --v2.0 start
               IF line_cur.line_type = 'LINE'
               THEN
                  /*l_adj_rec.acctd_amount := (-1) *
                                            ROUND(line_cur.amount,
                                                  l_precision);  */     -- Commented for v4.0

          l_adj_rec.acctd_amount := (-1) *
                                            ROUND(l_total_line_amount,
                                                  l_precision);      -- Added for v4.0
               ELSE
                  --- line_cur.line_type = 'TAX'
                  l_adj_rec.acctd_amount := (-1) *
                                            ROUND(rec_trx.amount_to_adjust -
                                                  l_total_line_amount,
                                                  l_precision);
               END IF; --end of line_cur.line_type = 'LINE' -
               --v2.0 end
               l_adj_rec.adjustment_id     := NULL;
               l_adj_rec.adjustment_number := NULL;
               --l_adj_rec.ADJUSTMENT_TYPE      := 'M';
               /* l_adj_rec.amount :=
               (-1) * ROUND (rec_trx.amount_to_adjust, l_precision);*/ --commented for v2.0
               --v2.0 start
               IF line_cur.line_type = 'LINE'
               THEN
                  /*l_adj_rec.amount := (-1) *
                                      ROUND(line_cur.amount, l_precision);  */  -- Commented for v4.0
          l_adj_rec.amount := (-1) *
                                      ROUND(l_total_line_amount, l_precision);  -- Added for v4.0
               ELSE
                  --- line_cur.line_type = 'TAX'
                  l_adj_rec.amount := (-1) *
                                      ROUND(rec_trx.amount_to_adjust -
                                            l_total_line_amount, l_precision);
               END IF; --end of line_cur.line_type = 'LINE' -
               --v2.0 end
               l_adj_rec.created_by       := g_user_id;
               l_adj_rec.created_from     := g_created_from;
               l_adj_rec.creation_date    := SYSDATE;
               l_adj_rec.gl_date          := TO_DATE(piv_adj_date,
                                                     'RRRR/MM/DD HH24:MI:SS');
               l_adj_rec.last_update_date := SYSDATE;
               l_adj_rec.last_updated_by  := g_user_id;
               l_adj_rec.set_of_books_id  := rec_trx.set_of_books_id;
               l_adj_rec.status           := 'A';
               -- l_adj_rec.TYPE := rec_trx.TYPE; commected for v2.0
               l_adj_rec.TYPE                := line_cur.line_type; --added for v2.0
               l_adj_rec.payment_schedule_id := rec_trx.payment_schedule_id;
               l_adj_rec.apply_date          := TO_DATE(piv_adj_date,
                                                        'RRRR/MM/DD HH24:MI:SS');
               BEGIN
                  SELECT receivables_trx_id
                        ,code_combination_id
                    INTO l_adj_rec.receivables_trx_id
                        ,l_ccid
                    FROM ar_receivables_trx_all
                   WHERE NAME = piv_rec_trx_act
                     AND org_id = rec_trx.org_id;
                  IF l_ccid IS NULL
                  THEN
                     l_line_status  := 'F';
                     l_failed_count := l_failed_count + 1;
                     l_message      := 'Adjustment Failed on Transaction Number: ' ||
                                       rec_trx.trx_number ||
                                       ' because, Acivity ' ||
                                       piv_rec_trx_act ||
                                       ' validation failed with error: Code Combination is not available on Adjustment Activity';
                     print_log_message('Error message:' || l_message);
                     xxetn_debug_pkg.add_debug(l_message);
                     pon_retcode := 1;
                     --  raise l_err_excep;
                  END IF;
               EXCEPTION
                  WHEN OTHERS THEN
                     l_line_status  := 'F';
                     l_failed_count := l_failed_count + 1;
                     l_message      := 'Adjustment Failed on Transaction Number: ' ||
                                       rec_trx.trx_number ||
                                       ' because, Acivity ' ||
                                       piv_rec_trx_act ||
                                       ' validation failed with error: ' ||
                                       SUBSTR(SQLERRM, 1, 150);
                     print_log_message('Error message:' || l_message);
                     xxetn_debug_pkg.add_debug(l_message);
                     pon_retcode := 1;
               END;
               l_adj_rec.code_combination_id := l_ccid;
               l_adj_rec.customer_trx_id     := rec_trx.customer_trx_id;
               IF l_line_status <> 'F'
                  AND rec_trx.amount_to_adjust <> 0
                  AND l_adj_rec.amount <> 0
               THEN
                  ar_adjust_pub.create_adjustment(p_api_name => l_api_name,
                                                  p_api_version => l_api_version,
                                                  p_init_msg_list => l_init_msg_list,
                                                  p_commit_flag => l_commit_flag,
                                                  p_validation_level => l_validation_level,
                                                  p_msg_count => l_msg_count,
                                                  p_msg_data => l_msg_data,
                                                  p_return_status => l_return_status,
                                                  p_adj_rec => l_adj_rec,
                                                  p_chk_approval_limits => l_chk_approval_limits,
                                                  p_check_amount => l_check_amount,
                                                  p_move_deferred_tax => l_move_deferred_tax,
                                                  p_new_adjust_number => l_new_adjust_number,
                                                  p_new_adjust_id => l_new_adjust_id,
                                                  p_called_from => l_called_from,
                                                  p_old_adjust_id => l_old_adjust_id,
                                                  p_org_id => rec_trx.org_id);
                  IF l_return_status <> fnd_api.g_ret_sts_success
                  THEN
                     l_failed_count := l_failed_count + 1;
                     pon_retcode    := 1;
                     IF l_msg_count = 1
                     THEN
                        l_message := l_msg_data;
                        fnd_file.put_line(fnd_file.LOG, l_msg_data);
                        xxetn_debug_pkg.add_debug(l_message);
                     ELSIF l_msg_count > 1
                     THEN
                        FOR i IN 1 .. l_msg_count
                        LOOP
                           l_message := l_message || i || '. ' ||
                                        SUBSTR(fnd_msg_pub.get(p_encoded => fnd_api.g_false),
                                               1, 255) || CHR(10);
                           fnd_file.put_line(fnd_file.LOG,
                                             i || '. ' ||
                                              SUBSTR(fnd_msg_pub.get(p_encoded => fnd_api.g_false),
                                                     1, 255));
                           xxetn_debug_pkg.add_debug(l_message);
                        END LOOP;
                     END IF;
                     fnd_file.put_line(fnd_file.LOG,
                                       'Adjustment Failed on Transaction Number: ' ||
                                        rec_trx.trx_number);
                     l_line_status    := 'F';
                     l_adj_status     := 'W';
                     l_trx_adj_status := 'W'; --ver3.1 changes
                     log_errors(piv_source_column_name => 'LEGACY_CUSTOMER_TRX_ID',
                                piv_source_column_value => rec_trx.leg_customer_trx_id,
                                piv_source_keyname1 => 'TRX_NUMBER',
                                piv_source_keyvalue1 => rec_trx.trx_number,
                                piv_source_keyname2 => 'ORG_ID',
                                piv_source_keyvalue2 => rec_trx.org_id,
                                piv_error_type => 'ERR_IMP',
                                piv_error_code => 'ETN_AR_ADJUSTMENT_ERROR',
                                piv_error_message => l_message,
                                pov_return_status => l_log_ret_status,
                                pov_error_msg => l_log_err_msg);
                  ELSE
                     l_success_count := l_success_count + 1;
                     fnd_file.put_line(fnd_file.LOG,
                                       'New Adjustment Number: ' ||
                                        l_new_adjust_number);
                     fnd_file.put_line(fnd_file.LOG,
                                       'New Adjustment ID: ' ||
                                        l_new_adjust_id);
                     fnd_file.put_line(fnd_file.LOG,
                                       'Adjustment Successfully Applied on Transaction Number: ' ||
                                        rec_trx.trx_number || ' and type ' ||
                                        rec_trx.TYPE);
                     xxetn_debug_pkg.add_debug('New Adjustment Number: ' ||
                                               l_new_adjust_number);
                     xxetn_debug_pkg.add_debug('New Adjustment ID: ' ||
                                               l_new_adjust_id);
                     xxetn_debug_pkg.add_debug('Adjustment Successfully Applied on Transaction Number: ' ||
                                               rec_trx.trx_number ||
                                               ' and type ' ||
                                               rec_trx.TYPE);
                     l_line_status := 'S';
                     l_adj_status  := 'C';
                  END IF;
               ELSIF rec_trx.amount_to_adjust = 0
               THEN
                  l_line_status   := 'S';
                  l_adj_status    := 'C';
                  l_success_count := l_success_count + 1;
               END IF;
            END LOOP; --end of line_cur for 2 adjustments
            --updating process flag
            BEGIN
               UPDATE xxar_invoices_stg
               --ver3.1 changes start
               --SET process_flag     = l_adj_status
                  SET process_flag = l_trx_adj_status
                      --ver3.1 changes end
                     ,last_update_date = SYSDATE
                WHERE process_flag = 'W'
                  AND leg_trx_number = rec_trx.trx_number
                     --ver3.2 changes start
                  AND leg_customer_trx_id = rec_trx.leg_customer_trx_id
                     --ver3.2 changes end
                     --AND leg_line_type = rec_trx.TYPE  --ver3.1
                  AND org_id = rec_trx.org_id;
            EXCEPTION
               WHEN OTHERS THEN
                  pon_retcode := 1;
                  pov_errbuf  := 'Failed During update';
                  xxetn_debug_pkg.add_debug('Error during update for process flag after adjustment ' ||
                                            SUBSTR(SQLERRM, 1, 150));
                  fnd_file.put_line(fnd_file.LOG,
                                    'Error ' || SUBSTR(SQLERRM, 1, 150));
                  log_errors(piv_source_column_name => 'LEGACY_CUSTOMER_TRX_ID',
                             piv_source_column_value => rec_trx.leg_customer_trx_id,
                             piv_source_keyname1 => 'TRX_NUMBER',
                             piv_source_keyvalue1 => rec_trx.trx_number,
                             piv_source_keyname2 => 'ORG_ID',
                             piv_source_keyvalue2 => rec_trx.org_id,
                             piv_error_type => 'ERR_IMP',
                             piv_error_code => 'ETN_AR_ADJUSTMENT_ERROR',
                             piv_error_message => 'Error while updating process_flag',
                             pov_return_status => l_log_ret_status,
                             pov_error_msg => l_log_err_msg);
            END;
            COMMIT;
            --end of update process flag
         ELSE
            --case where need to create 1 adjustment line
            l_line_status := 'S';
            --ver3.1 changes start
            --If for an invoice with installements, invoice must be marked W even if one installment fails
            IF l_leg_customer_trx_id <> rec_trx.leg_customer_trx_id
            THEN
               l_trx_adj_status      := 'C';
               l_leg_customer_trx_id := rec_trx.leg_customer_trx_id;
            END IF;
            --ver3.1 changes end
            l_adj_status  := 'C';
            l_message     := NULL;
            l_ccid        := NULL;
            l_total_count := l_total_count + 1;
            mo_global.init('AR');
            mo_global.set_org_context(rec_trx.org_id, '', 'S');
            l_adj_rec             := NULL;
            l_api_name            := NULL;
            l_api_version         := 1.0;
            l_called_from         := NULL;
            l_check_amount        := NULL;
            l_chk_approval_limits := 'F';
            l_commit_flag         := NULL;
            l_init_msg_list       := fnd_api.g_true;
            l_move_deferred_tax   := 'Y';
            l_msg_count           := 0;
            l_msg_data            := NULL;
            l_new_adjust_id       := NULL;
            l_new_adjust_number   := NULL;
            l_old_adjust_id       := NULL;
            l_return_status       := NULL;
            l_validation_level    := fnd_api.g_valid_level_full;
            BEGIN
               SELECT PRECISION
                 INTO l_precision
                 FROM fnd_currencies fc
                WHERE currency_code = rec_trx.leg_currency_code
                  AND ROWNUM = 1;
            EXCEPTION
               WHEN OTHERS THEN
                  l_precision := 2;
            END;
            l_adj_rec.acctd_amount      := (-1) * ROUND(rec_trx.amount_to_adjust,
                                                        l_precision);
            l_adj_rec.adjustment_id     := NULL;
            l_adj_rec.adjustment_number := NULL;
            --l_adj_rec.ADJUSTMENT_TYPE      := 'M';
            l_adj_rec.amount              := (-1) *
                                             ROUND(rec_trx.amount_to_adjust,
                                                   l_precision);
            l_adj_rec.created_by          := g_user_id;
            l_adj_rec.created_from        := g_created_from;
            l_adj_rec.creation_date       := SYSDATE;
            l_adj_rec.gl_date             := TO_DATE(piv_adj_date,
                                                     'RRRR/MM/DD HH24:MI:SS');
            l_adj_rec.last_update_date    := SYSDATE;
            l_adj_rec.last_updated_by     := g_user_id;
            l_adj_rec.set_of_books_id     := rec_trx.set_of_books_id;
            l_adj_rec.status              := 'A';
            l_adj_rec.TYPE                := rec_trx.TYPE;
            l_adj_rec.payment_schedule_id := rec_trx.payment_schedule_id;
            l_adj_rec.apply_date          := TO_DATE(piv_adj_date,
                                                     'RRRR/MM/DD HH24:MI:SS');
            BEGIN
               SELECT receivables_trx_id
                     ,code_combination_id
                 INTO l_adj_rec.receivables_trx_id
                     ,l_ccid
                 FROM ar_receivables_trx_all
                WHERE NAME = piv_rec_trx_act
                  AND org_id = rec_trx.org_id;
               IF l_ccid IS NULL
               THEN
                  l_line_status  := 'F';
                  l_failed_count := l_failed_count + 1;
                  l_message      := 'Adjustment Failed on Transaction Number: ' ||
                                    rec_trx.trx_number ||
                                    ' because, Acivity ' || piv_rec_trx_act ||
                                    ' validation failed with error: Code Combination is not available on Adjustment Activity';
                  print_log_message('Error message:' || l_message);
                  xxetn_debug_pkg.add_debug(l_message);
                  pon_retcode := 1;
                  --  raise l_err_excep;
               END IF;
            EXCEPTION
               WHEN OTHERS THEN
                  l_line_status  := 'F';
                  l_failed_count := l_failed_count + 1;
                  l_message      := 'Adjustment Failed on Transaction Number: ' ||
                                    rec_trx.trx_number ||
                                    ' because, Acivity ' || piv_rec_trx_act ||
                                    ' validation failed with error: ' ||
                                    SUBSTR(SQLERRM, 1, 150);
                  print_log_message('Error message:' || l_message);
                  xxetn_debug_pkg.add_debug(l_message);
                  pon_retcode := 1;
            END;
            l_adj_rec.code_combination_id := l_ccid;
            l_adj_rec.customer_trx_id     := rec_trx.customer_trx_id;
            IF l_line_status <> 'F'
               AND rec_trx.amount_to_adjust <> 0
            THEN
               ar_adjust_pub.create_adjustment(p_api_name => l_api_name,
                                               p_api_version => l_api_version,
                                               p_init_msg_list => l_init_msg_list,
                                               p_commit_flag => l_commit_flag,
                                               p_validation_level => l_validation_level,
                                               p_msg_count => l_msg_count,
                                               p_msg_data => l_msg_data,
                                               p_return_status => l_return_status,
                                               p_adj_rec => l_adj_rec,
                                               p_chk_approval_limits => l_chk_approval_limits,
                                               p_check_amount => l_check_amount,
                                               p_move_deferred_tax => l_move_deferred_tax,
                                               p_new_adjust_number => l_new_adjust_number,
                                               p_new_adjust_id => l_new_adjust_id,
                                               p_called_from => l_called_from,
                                               p_old_adjust_id => l_old_adjust_id,
                                               p_org_id => rec_trx.org_id);
               IF l_return_status <> fnd_api.g_ret_sts_success
               THEN
                  l_failed_count := l_failed_count + 1;
                  pon_retcode    := 1;
                  IF l_msg_count = 1
                  THEN
                     l_message := l_msg_data;
                     fnd_file.put_line(fnd_file.LOG, l_msg_data);
                     xxetn_debug_pkg.add_debug(l_message);
                  ELSIF l_msg_count > 1
                  THEN
                     FOR i IN 1 .. l_msg_count
                     LOOP
                        l_message := l_message || i || '. ' ||
                                     SUBSTR(fnd_msg_pub.get(p_encoded => fnd_api.g_false),
                                            1, 255) || CHR(10);
                        fnd_file.put_line(fnd_file.LOG,
                                          i || '. ' ||
                                           SUBSTR(fnd_msg_pub.get(p_encoded => fnd_api.g_false),
                                                  1, 255));
                        xxetn_debug_pkg.add_debug(l_message);
                     END LOOP;
                  END IF;
                  fnd_file.put_line(fnd_file.LOG,
                                    'Adjustment Failed on Transaction Number: ' ||
                                     rec_trx.trx_number);
                  l_line_status    := 'F';
                  l_adj_status     := 'W';
                  l_trx_adj_status := 'W'; --ver3.1 changes
                  log_errors(piv_source_column_name => 'LEGACY_CUSTOMER_TRX_ID',
                             piv_source_column_value => rec_trx.leg_customer_trx_id,
                             piv_source_keyname1 => 'TRX_NUMBER',
                             piv_source_keyvalue1 => rec_trx.trx_number,
                             piv_source_keyname2 => 'ORG_ID',
                             piv_source_keyvalue2 => rec_trx.org_id,
                             piv_error_type => 'ERR_IMP',
                             piv_error_code => 'ETN_AR_ADJUSTMENT_ERROR',
                             piv_error_message => l_message,
                             pov_return_status => l_log_ret_status,
                             pov_error_msg => l_log_err_msg);
               ELSE
                  l_success_count := l_success_count + 1;
                  fnd_file.put_line(fnd_file.LOG,
                                    'New Adjustment Number: ' ||
                                     l_new_adjust_number);
                  fnd_file.put_line(fnd_file.LOG,
                                    'New Adjustment ID: ' ||
                                     l_new_adjust_id);
                  fnd_file.put_line(fnd_file.LOG,
                                    'Adjustment Successfully Applied on Transaction Number: ' ||
                                     rec_trx.trx_number || ' and type ' ||
                                     rec_trx.TYPE);
                  xxetn_debug_pkg.add_debug('New Adjustment Number: ' ||
                                            l_new_adjust_number);
                  xxetn_debug_pkg.add_debug('New Adjustment ID: ' ||
                                            l_new_adjust_id);
                  xxetn_debug_pkg.add_debug('Adjustment Successfully Applied on Transaction Number: ' ||
                                            rec_trx.trx_number ||
                                            ' and type ' || rec_trx.TYPE);
                  l_line_status := 'S';
                  l_adj_status  := 'C';
               END IF;
            ELSIF rec_trx.amount_to_adjust = 0
            THEN
               l_line_status   := 'S';
               l_adj_status    := 'C';
               l_success_count := l_success_count + 1;
            END IF;
            BEGIN
               UPDATE xxar_invoices_stg
               --ver3.1 changes start
               --SET process_flag     = l_adj_status
                  SET process_flag = l_trx_adj_status
                      --ver3.1 changes end
                     ,last_update_date = SYSDATE
                WHERE process_flag = 'W'
                  AND leg_trx_number = rec_trx.trx_number
                     --ver3.2 changes start
                  AND leg_customer_trx_id = rec_trx.leg_customer_trx_id
                     --ver3.2 changes end
                     --AND leg_line_type = rec_trx.TYPE --ver3.1
                  AND org_id = rec_trx.org_id;
            EXCEPTION
               WHEN OTHERS THEN
                  pon_retcode := 1;
                  pov_errbuf  := 'Failed During update';
                  xxetn_debug_pkg.add_debug('Error during update for process flag after adjustment ' ||
                                            SUBSTR(SQLERRM, 1, 150));
                  fnd_file.put_line(fnd_file.LOG,
                                    'Error ' || SUBSTR(SQLERRM, 1, 150));
                  log_errors(piv_source_column_name => 'LEGACY_CUSTOMER_TRX_ID',
                             piv_source_column_value => rec_trx.leg_customer_trx_id,
                             piv_source_keyname1 => 'TRX_NUMBER',
                             piv_source_keyvalue1 => rec_trx.trx_number,
                             piv_source_keyname2 => 'ORG_ID',
                             piv_source_keyvalue2 => rec_trx.org_id,
                             piv_error_type => 'ERR_IMP',
                             piv_error_code => 'ETN_AR_ADJUSTMENT_ERROR',
                             piv_error_message => 'Error while updating process_flag',
                             pov_return_status => l_log_ret_status,
                             pov_error_msg => l_log_err_msg);
            END;
            COMMIT;
         END IF; --v2.0 end of IF rec_trx.amount_to_adjust> nvl(l_total_line_amount,0)
      END LOOP;
      IF g_error_tab.COUNT > 0
      THEN
         xxetn_common_error_pkg.add_error(pov_return_status => l_log_ret_status,
                                          pov_error_msg => l_log_err_msg,
                                          pi_source_tab => g_error_tab,
                                          pin_batch_id => g_batch_id);
      END IF;
   EXCEPTION
      WHEN l_err_excep THEN
         fnd_file.put_line(fnd_file.LOG,
                           'Main program procedure encounter user exception ' ||
                            SUBSTR(SQLERRM, 1, 150));
         pov_errbuf  := 'Error : Main program procedure encounter user exception. ' ||
                        SUBSTR(SQLERRM, 1, 150);
         pon_retcode := 2;
      WHEN OTHERS THEN
         fnd_file.put_line(fnd_file.LOG,
                           'Main program procedure encounter error ' ||
                            SUBSTR(SQLERRM, 1, 150));
         pon_retcode := 2;
         pov_errbuf  := 'Failed During update';
         fnd_file.put_line(fnd_file.LOG,
                           'Error ' || SUBSTR(SQLERRM, 1, 150));
   END MAIN;

END xxar_invoices_adjust_pkg;
/

--Begin Revision History
--<<<
-- 26-Mar-2017 22:24:44 C9987884 /main/22
-- 
--<<<
--End Revision History  
CREATE OR REPLACE PACKAGE BODY xxpa_prjtsk_cnv_pkg
------------------------------------------------------------------------------------------
--    Owner        : EATON CORPORATION.
--    Application  : Projects
--    Schema       : APPS
--    Compile AS   : APPS
--    File Name    : XXPA_PRJTSK_CNV_PKG.pkb
--    Date         : 07-Apr-2014
--    Author       : Manoj Sharma
--    Description  : Package Body for GL Balance conversion
--
--    Version      : $ETNHeader: /CCSTORE/ccweb/C9987884/C9987884_PA_TOP/vobs/PA_TOP/xxpa/12.0.0/install/XXPA_PRJTSK_CNV_PKG.pkb /main/22 26-Mar-2017 22:24:44 C9987884  $
--
--    Parameters  :
--
--    Change History
--    Version     Created By       Date            Comments
--  ======================================================================================
--    1.0         Manoj Sharma         07-Apr-2014     Initial Creation
--    1.1         Harjinder Singh      16-Apr-2015     Changed the procedure derive Organization
--    1.3         Shailesh Chaudhari   26-Mar-2017     For defect #15763 Added for project template derviation
--                                                     This is considered as enhancement as gthere is 
--                                                     change in approach to derive template.
--  ======================================================================================
------------------------------------------------------------------------------------------
AS
-- -----------------------------------------------------------------
-- Package level Global Placeholders
-- -----------------------------------------------------------------

   /* Program completion codes */
   g_normal                     CONSTANT NUMBER                     := 0;
   g_warning                    CONSTANT NUMBER                     := 1;
   g_error                      CONSTANT NUMBER                     := 2;
   /* Run Mode constants*/
   g_run_mode_loadata           CONSTANT VARCHAR2 (9)              := 'LOAD-DATA';
   g_run_mode_prevalidate       CONSTANT VARCHAR2 (12)          := 'PRE-VALIDATE';
   g_run_mode_validate          CONSTANT VARCHAR2 (8)             := 'VALIDATE';
   g_run_mode_conversion        CONSTANT VARCHAR2 (10)            := 'CONVERSION';
   g_run_mode_reconcilition     CONSTANT VARCHAR2 (14)             := 'RECONCILE';
   /* Process records constants */
   g_process_recs_all           CONSTANT VARCHAR2 (3)               := 'ALL';
   g_process_recs_error         CONSTANT VARCHAR2 (5)               := 'ERROR';
   g_process_recs_unprocessed   CONSTANT VARCHAR2 (11)           := 'UNPROCESSED';
   /* Flag constants */
   g_flag_ntprocessed           CONSTANT VARCHAR2 (1)               := 'N';
   g_flag_validated             CONSTANT VARCHAR2 (1)               := 'V';
   g_flag_processed             CONSTANT VARCHAR2 (1)               := 'P';
   g_flag_completed             CONSTANT VARCHAR2 (1)               := 'C';
   g_flag_success               CONSTANT VARCHAR2 (1)               := 'S';
   g_flag_error                 CONSTANT VARCHAR2 (1)               := 'E';
   g_flag_obsolete              CONSTANT VARCHAR2 (1)               := 'X';
   g_flag_yes                   CONSTANT VARCHAR2 (1)               := 'Y';
   g_flag_no                    CONSTANT VARCHAR2 (1)               := 'N';
   /* Lookup constants */
   g_common_ou_map              CONSTANT VARCHAR2 (17)         := 'ETN_COMMON_OU_MAP';
   /* Other constants */
   g_project_class_category     CONSTANT VARCHAR2 (12)          := 'Project Type';
   g_source_table               CONSTANT VARCHAR2 (22)           := 'XXPA_OPEN_PROJECTS_STG';
   g_project_template           CONSTANT VARCHAR2 (99)        := 'Capital Projects';
   g_err_val                    CONSTANT VARCHAR2 (7)              := 'ERR_VAL';
   g_err_imp                    CONSTANT VARCHAR2 (7)              := 'ERR_IMP';
   g_prj_sts_approved           CONSTANT VARCHAR2 (8)             := 'APPROVED';
   g_prj_sts_unapproved         CONSTANT VARCHAR2 (10)            := 'UNAPPROVED';
   g_prj_sts_etnunapproved    CONSTANT VARCHAR2 (15)            := 'ETN Unapproved';
   /* Global variables */
   g_run_mode                            VARCHAR2 (14)              := NULL;
   g_batch_id                            NUMBER                     := NULL;
   g_process_records                     VARCHAR2 (11)              := NULL;
   g_new_batch_id                        NUMBER                     := NULL;
   g_run_sequence_id                     NUMBER                     := NULL;
   g_limit                               NUMBER                   := fnd_profile.VALUE ('ETN_FND_ERROR_TAB_LIMIT');
   g_bulk_exception                      EXCEPTION;
   PRAGMA EXCEPTION_INIT (g_bulk_exception, -24381);
   g_project_template_id                 NUMBER;
   /* WHO Columns */
   g_request_id                          NUMBER    := fnd_global.conc_request_id;
   g_prog_appl_id                        NUMBER    := fnd_global.prog_appl_id;
   g_program_id                          NUMBER    := fnd_global.conc_program_id;
   g_user_id                             NUMBER    := fnd_global.user_id;
   g_login_id                            NUMBER    := fnd_global.login_id;
   g_org_id                              NUMBER    := fnd_global.org_id;
   g_resp_id                             NUMBER    := fnd_global.resp_id;
   g_resp_appl_id             NUMBER    := fnd_global.resp_appl_id;
   /* Record Types and Table Types */
   TYPE g_pass_fail_projects_rec IS RECORD (
      interface_txn_id   xxpa_open_projects_stg.interface_txn_id%TYPE,
      process_flag       xxpa_open_projects_stg.process_flag%TYPE,
      ERROR_TYPE         xxpa_open_projects_stg.ERROR_TYPE%TYPE,
      MESSAGE            VARCHAR2 (2000)
   );

   TYPE pass_fail_projects_ttype IS TABLE OF g_pass_fail_projects_rec
      INDEX BY BINARY_INTEGER;

   g_pass_fail_projects_ttype            pass_fail_projects_ttype;

-- =============================================================================
-- Procedure: debug
-- =============================================================================
--   Common procedure to pring message to concurrent program log
-- =============================================================================
--  Input Parameters :
--    p_message  : Message Text
--  Output Parameters :
--    No Output parameters
-- -----------------------------------------------------------------------------
   PROCEDURE DEBUG (p_message IN VARCHAR2)
   IS
      l_error_message   VARCHAR2 (2000);
   BEGIN
      xxetn_debug_pkg.add_debug (piv_debug_msg => p_message);
   EXCEPTION
      WHEN OTHERS
      THEN
         l_error_message :=
            SUBSTR
               (   'Exception in Procedure XXPA_PRJTSK_CNV_PKG.debug. SQLERRM '
                || SQLERRM,
                1,
                2000
               );
         fnd_file.put_line (fnd_file.LOG, l_error_message);
   END DEBUG;

-- =============================================================================
-- Procedure: log_error
-- =============================================================================
--   Common procedure to insert record into common error table
-- =============================================================================
--  Input Parameters :
--    pin_batch_id        : Batch Id
--    pin_run_sequence_id : Run Requence Id
--    p_source_tab_type   : Error Table Type (xxetn_common_error_pkg.g_source_tab_type)
--  Output Parameters :
--    pov_return_status    :
--    pov_error_message    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   PROCEDURE log_error (
      p_source_tab_type   IN              xxetn_common_error_pkg.g_source_tab_type,
      pov_return_status   OUT NOCOPY      VARCHAR2,
      pov_error_message   OUT NOCOPY      VARCHAR2
   )
   IS
      process_exception   EXCEPTION;
      l_return_status     VARCHAR2 (1)    := NULL;
      l_error_message     VARCHAR2 (2000) := NULL;
   BEGIN
      xxetn_common_error_pkg.add_error
                                   (pov_return_status        => l_return_status,
                                    pov_error_msg            => l_error_message,
                                    pin_batch_id             => g_new_batch_id,
                                    pin_iface_load_id        => NULL,
                                    pin_run_sequence_id      => g_run_sequence_id,
                                    pi_source_tab            => p_source_tab_type,
                                    piv_active_flag          => g_flag_yes,
                                    pin_program_id           => g_program_id,
                                    pin_request_id           => g_request_id
                                   );

      IF l_error_message IS NOT NULL
      THEN
         RAISE process_exception;
      END IF;
   EXCEPTION
      WHEN process_exception
      THEN
         pov_return_status := g_error;
         pov_error_message := l_error_message;
         DEBUG (l_error_message);
      WHEN OTHERS
      THEN
         l_error_message :=
            SUBSTR ('Exception in Procedure log_error. ' || SQLERRM, 1, 1999);
         pov_return_status := g_error;
         pov_error_message := l_error_message;
         DEBUG (l_error_message);
   END log_error;

-- =============================================================================
-- Procedure: print_report
-- =============================================================================
--   To print program stats at the end of each run and also executed when program
--   is submitted in RECONCILE mode
-- =============================================================================
--  Input Parameters :
--    pin_batch_id        : Batch Id
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   PROCEDURE print_report (
      p_errbuf     OUT      VARCHAR2,
      p_retcode    OUT      NUMBER,
      p_batch_id   IN       NUMBER

   )
   IS
      l_error_message   VARCHAR2 (2000);
      l_total           NUMBER          := 0;
      l_validated       NUMBER          := 0;
      l_complete        NUMBER          := 0;
      l_val_error       NUMBER          := 0;
      l_imp_error       NUMBER          := 0;
   BEGIN
      p_retcode := g_normal;
      p_errbuf := NULL;
      DEBUG ('p_batch_id : ' || p_batch_id || CHR (10));

      BEGIN

      if g_run_mode = g_run_mode_validate   THEN
         --if g_batch_id is null then

            SELECT COUNT (*)
            INTO l_total
            FROM xxpa_open_projects_stg xops
            WHERE xops.batch_id = NVL (p_batch_id, batch_id)
            and process_flag = decode(g_process_records,'ERROR','E','UNPROCESSED','N','ALL',process_flag);

         else

            SELECT COUNT (*)
            INTO l_total
            FROM xxpa_open_projects_stg xops
            WHERE xops.batch_id = NVL (p_batch_id, batch_id)
            AND xops.run_sequence_id = NVL (g_run_sequence_id, xops.run_sequence_id);
        -- end if;

      end if;
        /* SELECT COUNT (*)
           INTO l_total
           FROM xxpa_open_projects_stg xops
          WHERE xops.batch_id = NVL (p_batch_id, batch_id)
            AND xops.run_sequence_id =
                                 NVL (g_run_sequence_id, xops.run_sequence_id);*/
      EXCEPTION
         WHEN OTHERS
         THEN
            DEBUG ('Exception occured while fetching total no. of records');
      END;

      BEGIN

       if g_run_mode = g_run_mode_validate   THEN
        --if g_batch_id is null then

           SELECT COUNT (*)
           INTO l_validated
           FROM xxpa_open_projects_stg xops
          WHERE xops.batch_id = NVL (p_batch_id, batch_id)
            AND xops.process_flag = g_flag_validated;

         else

           SELECT COUNT (*)
           INTO l_validated
           FROM xxpa_open_projects_stg xops
          WHERE xops.batch_id = NVL (p_batch_id, batch_id)
            AND xops.process_flag = g_flag_validated
            AND xops.run_sequence_id =
                                 NVL (g_run_sequence_id, xops.run_sequence_id);
         --end if;

      end if;
        /* SELECT COUNT (*)
           INTO l_validated
           FROM xxpa_open_projects_stg xops
          WHERE xops.batch_id = NVL (p_batch_id, batch_id)
            AND xops.process_flag = g_flag_validated
            AND xops.run_sequence_id =
                                 NVL (g_run_sequence_id, xops.run_sequence_id);*/
      EXCEPTION
         WHEN OTHERS
         THEN
            DEBUG
                 ('Exception occured while fetching validated no. of records');
      END;

      BEGIN

       if g_run_mode = g_run_mode_validate   THEN
        -- if g_batch_id is null then

  SELECT COUNT (*)
           INTO l_complete
           FROM xxpa_open_projects_stg xops
          WHERE xops.batch_id = NVL (p_batch_id, batch_id)
            AND xops.process_flag = g_flag_completed;

         else

           SELECT COUNT (*)
           INTO l_complete
           FROM xxpa_open_projects_stg xops
          WHERE xops.batch_id = NVL (p_batch_id, batch_id)
            AND xops.process_flag = g_flag_completed
            AND xops.run_sequence_id =
                                 NVL (g_run_sequence_id, xops.run_sequence_id);
        -- end if;

      end if;
         /*SELECT COUNT (*)
           INTO l_complete
           FROM xxpa_open_projects_stg xops
          WHERE xops.batch_id = NVL (p_batch_id, batch_id)
            AND xops.process_flag = g_flag_completed
            AND xops.run_sequence_id =
                                 NVL (g_run_sequence_id, xops.run_sequence_id);*/
      EXCEPTION
         WHEN OTHERS
         THEN
            DEBUG
                 ('Exception occured while fetching converted no. of records');
      END;

      BEGIN

           if g_run_mode = g_run_mode_validate   THEN
        -- if g_batch_id is null then

   SELECT COUNT (*)
           INTO l_val_error
           FROM xxpa_open_projects_stg xops
          WHERE xops.batch_id = NVL (p_batch_id, batch_id)
            AND xops.process_flag = g_flag_error
            AND xops.ERROR_TYPE = g_err_val;

         else

           SELECT COUNT (*)
           INTO l_val_error
           FROM xxpa_open_projects_stg xops
          WHERE xops.batch_id = NVL (p_batch_id, batch_id)
            AND xops.process_flag = g_flag_error
            AND xops.run_sequence_id =
                                 NVL (g_run_sequence_id, xops.run_sequence_id)
            AND xops.ERROR_TYPE = g_err_val;
        -- end if;

      end if;
        /* SELECT COUNT (*)
           INTO l_val_error
           FROM xxpa_open_projects_stg xops
          WHERE xops.batch_id = NVL (p_batch_id, batch_id)
            AND xops.process_flag = g_flag_error
            AND xops.run_sequence_id =
                                 NVL (g_run_sequence_id, xops.run_sequence_id)
            AND xops.ERROR_TYPE = g_err_val;*/
      EXCEPTION
         WHEN OTHERS
         THEN
            DEBUG
               ('Exception occured while fetching validation error no. of records'
               );
      END;

      BEGIN

             if g_run_mode = g_run_mode_validate   THEN
       --  if g_batch_id is null then

   SELECT COUNT (*)
           INTO l_imp_error
           FROM xxpa_open_projects_stg xops
          WHERE xops.batch_id = NVL (p_batch_id, batch_id)
            AND xops.process_flag = g_flag_error
            AND xops.ERROR_TYPE = g_err_imp;

         else

         SELECT COUNT (*)
           INTO l_imp_error
           FROM xxpa_open_projects_stg xops
          WHERE xops.batch_id = NVL (p_batch_id, batch_id)
            AND xops.process_flag = g_flag_error
            AND xops.run_sequence_id =
                                 NVL (g_run_sequence_id, xops.run_sequence_id)
            AND xops.ERROR_TYPE = g_err_imp;
       --  end if;

      end if;
         /*SELECT COUNT (*)
           INTO l_imp_error
           FROM xxpa_open_projects_stg xops
          WHERE xops.batch_id = NVL (p_batch_id, batch_id)
            AND xops.process_flag = g_flag_error
            AND xops.run_sequence_id =
                                 NVL (g_run_sequence_id, xops.run_sequence_id)
            AND xops.ERROR_TYPE = g_err_imp;*/
      EXCEPTION
         WHEN OTHERS
         THEN
            DEBUG
               ('Exception occured while fetching import error no. of records'
               );
      END;

      fnd_file.put_line
            (fnd_file.output,
             'Program Name: Eaton Unify Projects and Tasks Conversion Program'
            );
      fnd_file.put_line (fnd_file.output, '  Request Id: ' || g_request_id);
      fnd_file.put_line (fnd_file.output,
                            ' Report Date: '
                         || TO_CHAR (SYSDATE, 'DD-MON-RRRR HH:MI:SS AM')
                        );
      fnd_file.put_line (fnd_file.output, ' ');
      fnd_file.put_line (fnd_file.output,
                         '.......................................'
                        );
      fnd_file.put_line (fnd_file.output, 'Program Parameters');
      fnd_file.put_line (fnd_file.output,
                         '.......................................'
                        );
      fnd_file.put_line (fnd_file.output, '       Run Mode : ' || g_run_mode);
      fnd_file.put_line (fnd_file.output, '       Batch Id : ' || g_batch_id);
      fnd_file.put_line (fnd_file.output,
                         'Process Records : ' || g_process_records
                        );
      fnd_file.put_line (fnd_file.output,
                         '.......................................'
                        );
      fnd_file.put_line (fnd_file.output, ' ');
      fnd_file.put_line (fnd_file.output, ' ');
      fnd_file.put_line (fnd_file.output,
                         '---------------------------------------'
                        );
      fnd_file.put_line (fnd_file.output, 'Records Status Stats');
      fnd_file.put_line (fnd_file.output,
                         '---------------------------------------'
                        );
      fnd_file.put_line (fnd_file.output,
                         '             Total Count : ' || l_total
                        );
      fnd_file.put_line (fnd_file.output,
                         '       Validated Records : ' || l_validated
                        );
      fnd_file.put_line (fnd_file.output,
                         'Validation Error Records : ' || l_val_error
                        );
      fnd_file.put_line (fnd_file.output,
                         '    Import Error Records : ' || l_imp_error
                        );
      fnd_file.put_line (fnd_file.output,
                         '       Converted Records : ' || l_complete
                        );
      fnd_file.put_line (fnd_file.output,
                         '---------------------------------------'
                        );
   EXCEPTION
      WHEN OTHERS
      THEN
         l_error_message :=
            SUBSTR
               (   'Exception in Procedure XXPA_PRJTSK_CNV_PKG.print_report. SQLERRM '
                || SQLERRM,
                1,
                2000
               );
         p_retcode := g_error;
         p_errbuf := l_error_message;
         DEBUG (p_errbuf);
   END print_report;

-- =============================================================================
-- Function: check_duplicate_project_num
-- =============================================================================
--   To check existence of record(s) for same Project Number in Staging table
--   and return TRUE/FALSE as Success/Failure
-- =============================================================================
--  Input Parameters :
--    p_project_num  : Project Number
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   FUNCTION check_duplicate_project_num (
      p_errbuf        OUT      VARCHAR2,
      p_retcode       OUT      NUMBER,
      p_project_num   IN       VARCHAR2,
      p_intf_txn_id   IN       NUMBER
   )
      RETURN BOOLEAN
   IS
      l_error_count     NUMBER := 0;
      l_project_count   NUMBER := 0;
   BEGIN
      DEBUG ('Function: check_duplicate_project_num');
      p_errbuf := NULL;
      p_retcode := g_normal;

      /* Checking for duplicate Project Number */
      IF p_project_num IS NULL
      THEN
         l_error_count := l_error_count + 1;
      ELSIF p_project_num IS NOT NULL
      THEN
         SELECT COUNT (*)
           INTO l_project_count
           FROM xxpa_open_projects_stg xops
          WHERE xops.leg_project_number = p_project_num
            AND xops.interface_txn_id <> p_intf_txn_id
            AND xops.batch_id = g_new_batch_id;

         IF l_project_count = 0
         THEN
            NULL;
         ELSE
            l_error_count := l_error_count + 1;
            p_errbuf :=
                  'Error : Record with same Project Number '
               || p_project_num
               || ' already exists in Staging table';
            p_retcode := g_warning;
            DEBUG (p_errbuf);
         END IF;
      END IF;

      IF l_error_count > 0
      THEN
         RETURN FALSE;
      ELSE
         RETURN TRUE;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR (   'Exception in Function check_duplicate_project_num. '
                    || SQLERRM,
                    1,
                    2000
                   );
         p_retcode := g_error;
         DEBUG (p_errbuf);
         RETURN FALSE;
   END check_duplicate_project_num;

-- =============================================================================
-- Function: check_duplicate_project_name
-- =============================================================================
--   To check existence of record(s) for same Project Name in Staging table
--   and return TRUE/FALSE as Success/Failure
-- =============================================================================
--  Input Parameters :
--    p_project_name  : Project Name
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   FUNCTION check_duplicate_project_name (
      p_errbuf         OUT      VARCHAR2,
      p_retcode        OUT      NUMBER,
      p_project_name   IN       VARCHAR2,
      p_intf_txn_id    IN       NUMBER
   )
      RETURN BOOLEAN
   IS
      l_error_count     NUMBER := 0;
      l_project_count   NUMBER := 0;
   BEGIN
      DEBUG ('Function: check_duplicate_project_name');
      p_errbuf := NULL;
      p_retcode := g_normal;

      /* Checking for duplicate Project Name */
      IF p_project_name IS NULL
      THEN
         l_error_count := l_error_count + 1;
      ELSIF p_project_name IS NOT NULL
      THEN
         SELECT COUNT (*)
           INTO l_project_count
           FROM xxpa_open_projects_stg xops
          WHERE xops.leg_project_name = p_project_name
            AND xops.interface_txn_id <> p_intf_txn_id
            AND xops.batch_id = g_new_batch_id;

         IF l_project_count = 0
         THEN
            NULL;
         ELSE
            l_error_count := l_error_count + 1;
            p_errbuf :=
                  'Error : Record with same Project Name '
               || p_project_name
               || ' already exists in Staging table';
            p_retcode := g_warning;
            DEBUG (p_errbuf);
         END IF;
      END IF;

      IF l_error_count > 0
      THEN
         RETURN FALSE;
      ELSE
         RETURN TRUE;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR
                  (   'Exception in Function check_duplicate_project_name. '
                   || SQLERRM,
                   1,
                   2000
                  );
         p_retcode := g_error;
         DEBUG (p_errbuf);
         RETURN FALSE;
   END check_duplicate_project_name;

-- =============================================================================
-- Function: check_duplicate_project_lname
-- =============================================================================
--   To check existence of record(s) for same Project Long Name in Staging table
--   and return TRUE/FALSE as Success/Failure
-- =============================================================================
--  Input Parameters :
--    p_project_l_name  : Project Long Name
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   FUNCTION check_duplicate_project_lname (
      p_errbuf           OUT      VARCHAR2,
      p_retcode          OUT      NUMBER,
      p_project_l_name   IN       VARCHAR2,
      p_intf_txn_id      IN       NUMBER
   )
      RETURN BOOLEAN
   IS
      l_error_count     NUMBER := 0;
      l_project_count   NUMBER := 0;
   BEGIN
      DEBUG ('Function: check_duplicate_project_lname');
      p_errbuf := NULL;
      p_retcode := g_normal;

      /* Checking for duplicate Project Long Name */
      IF p_project_l_name IS NULL
      THEN
         l_error_count := l_error_count + 1;
      ELSIF p_project_l_name IS NOT NULL
      THEN
         SELECT COUNT (*)
           INTO l_project_count
           FROM xxpa_open_projects_stg xops
          WHERE xops.leg_project_long_name = p_project_l_name
            AND xops.interface_txn_id <> p_intf_txn_id
            AND xops.batch_id = g_new_batch_id;

         IF l_project_count = 0
         THEN
            NULL;
         ELSE
            l_error_count := l_error_count + 1;
            p_errbuf :=
                  'Error : Record with same Project Long Name '
               || p_project_l_name
               || ' already exists in Staging table';
            p_retcode := g_warning;
            DEBUG (p_errbuf);
         END IF;
      END IF;

      IF l_error_count > 0
      THEN
         RETURN FALSE;
      ELSE
         RETURN TRUE;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR
                 (   'Exception in Function check_duplicate_project_lname. '
                  || SQLERRM,
                  1,
                  2000
                 );
         p_retcode := g_error;
         DEBUG (p_errbuf);
         RETURN FALSE;
   END check_duplicate_project_lname;

-- =============================================================================
-- Function: validate_project_num
-- =============================================================================
--   To Validate Project Number and return TRUE/FALSE as Success/Failure
-- =============================================================================
--  Input Parameters :
--    p_project_num  : Project Number
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   FUNCTION validate_project_num (
      p_errbuf        OUT      VARCHAR2,
      p_retcode       OUT      NUMBER,
      p_project_num   IN       VARCHAR2
   )
      RETURN BOOLEAN
   IS
      l_project_count   NUMBER := 0;
   BEGIN
      DEBUG ('Function: validate_project_num');

      IF p_project_num IS NULL
      THEN
         p_errbuf := 'Error : Project Number cannot be NULL';
         p_retcode := g_warning;
         DEBUG (p_errbuf);
         RETURN FALSE;
      ELSIF p_project_num IS NOT NULL
      THEN
         SELECT COUNT (*)
           INTO l_project_count
           FROM pa_projects_all ppa
          WHERE ppa.segment1 = p_project_num;

         IF l_project_count = 0
         THEN
            RETURN TRUE;
         ELSE
            p_errbuf := 'Error : Project Number already exist in Oracle';
            p_retcode := g_warning;
            DEBUG (p_errbuf);
            RETURN FALSE;
         END IF;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR ('Exception in Function validate_project_num. ' || SQLERRM,
                    1,
                    2000
                   );
         p_retcode := g_error;
         DEBUG (p_errbuf);
         RETURN FALSE;
   END validate_project_num;

-- =============================================================================
-- Function: validate_project_name
-- =============================================================================
--   To Validate Project Name and return TRUE/FALSE as Success/Failure
-- =============================================================================
--  Input Parameters :
--    p_project_name : Project Name
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   FUNCTION validate_project_name (
      p_errbuf         OUT      VARCHAR2,
      p_retcode        OUT      NUMBER,
      p_project_name   IN       VARCHAR2
   )
      RETURN BOOLEAN
   IS
      l_project_count   NUMBER := 0;
   BEGIN
      DEBUG ('Function: validate_project_name');

      IF p_project_name IS NULL
      THEN
         p_errbuf := 'Error : Project Name cannot be NULL';
         p_retcode := g_warning;
         DEBUG (p_errbuf);
         RETURN FALSE;
      ELSIF p_project_name IS NOT NULL
      THEN
         SELECT COUNT (*)
           INTO l_project_count
           FROM pa_projects_all ppa
          WHERE ppa.NAME = p_project_name;

         IF l_project_count = 0
         THEN
            RETURN TRUE;
         ELSE
            p_errbuf := 'Error : Project Name already exist in Oracle';
            p_retcode := g_warning;
            DEBUG (p_errbuf);
            RETURN FALSE;
         END IF;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR ('Exception in Function validate_project_name. ' || SQLERRM,
                    1,
                    2000
                   );
         p_retcode := g_error;
         DEBUG (p_errbuf);
         RETURN FALSE;
   END validate_project_name;

-- =============================================================================
-- Function: validate_project_long_name
-- =============================================================================
--   To Validate Project Long Name and return TRUE/FALSE as Success/Failure
-- =============================================================================
--  Input Parameters :
--    p_project_l_name : Project Long Name
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   FUNCTION validate_project_long_name (
      p_errbuf           OUT      VARCHAR2,
      p_retcode          OUT      NUMBER,
      p_project_l_name   IN       VARCHAR2
   )
      RETURN BOOLEAN
   IS
      l_project_count   NUMBER := 0;
   BEGIN
      DEBUG ('Function: validate_project_long_name');

      IF p_project_l_name IS NULL
      THEN
         p_errbuf := 'Error : Project Long Name cannot be NULL';
         p_retcode := g_warning;
         DEBUG (p_errbuf);
         RETURN FALSE;
      ELSE
         SELECT COUNT (*)
           INTO l_project_count
           FROM pa_projects_all ppa
          WHERE ppa.long_name = p_project_l_name;

         IF l_project_count = 0
         THEN
            RETURN TRUE;
         ELSE
            p_errbuf := 'Error : Project Long Name already exist in Oracle';
            p_retcode := g_warning;
            DEBUG (p_errbuf);
            RETURN FALSE;
         END IF;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR (   'Exception in Function validate_project_long_name. '
                    || SQLERRM,
                    1,
                    2000
                   );
         p_retcode := g_error;
         DEBUG (p_errbuf);
         RETURN FALSE;
   END validate_project_long_name;

-- =============================================================================
-- Function: validate_project_start_date
-- =============================================================================
--   To Validate Project Start Date and return TRUE/FALSE as Success/Failure
-- =============================================================================
--  Input Parameters :
--    p_project_start_date : Project Start Date
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   FUNCTION validate_project_start_date (
      p_errbuf               OUT      VARCHAR2,
      p_retcode              OUT      NUMBER,
      p_project_start_date   IN       VARCHAR2
   )
      RETURN BOOLEAN
   IS
   BEGIN
      DEBUG ('Function: validate_project_start_date');

      IF p_project_start_date IS NULL
      THEN
         p_errbuf := 'Error : Project Start Date cannot be NULL';
         p_retcode := g_warning;
         DEBUG (p_errbuf);
         RETURN FALSE;
      ELSE
         RETURN TRUE;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR (   'Exception in Function validate_project_start_date. '
                    || SQLERRM,
                    1,
                    2000
                   );
         p_retcode := g_error;
         DEBUG (p_errbuf);
         RETURN FALSE;
   END validate_project_start_date;

-- =============================================================================
-- Function: validate_project_type
-- =============================================================================
--   To Validate Project Type and return TRUE/FALSE as Success/Failure
-- =============================================================================
--  Input Parameters :
--    p_project_type       : Project Type
--    p_project_tempate_id : Project Template Id
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   FUNCTION validate_project_type (
      p_errbuf         OUT      VARCHAR2,
      p_retcode        OUT      NUMBER,
      p_project_type   IN       VARCHAR2
   )
      RETURN BOOLEAN
   IS
      l_count   NUMBER := 0;
   BEGIN
      DEBUG ('Function: validate_project_type');

      IF p_project_type IS NULL
      THEN
         p_errbuf := 'Error : Project Type cannot be NULL';
         p_retcode := g_warning;
         DEBUG (p_errbuf);
         RETURN FALSE;
      ELSE
         SELECT COUNT (*)
           INTO l_count
           FROM pa_class_codes pcc
          WHERE pcc.class_category = g_project_class_category
            AND UPPER (pcc.class_code) = UPPER (p_project_type)
            AND TRUNC (SYSDATE) BETWEEN NVL (pcc.start_date_active, SYSDATE)
                                    AND NVL (pcc.end_date_active, SYSDATE + 1);

         DEBUG ('Project Classification count ' || l_count);

         IF l_count = 0
         THEN
            p_errbuf :=
                      'Error : Project Type not Setup as Classification Code';
            p_retcode := g_error;
            DEBUG (p_errbuf);
            RETURN FALSE;
         ELSE
            RETURN TRUE;
         END IF;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR ('Exception in Function validate_project_type. ' || SQLERRM,
                    1,
                    2000
                   );
         p_retcode := g_error;
         DEBUG (p_errbuf);
         RETURN FALSE;
   END validate_project_type;

-- =============================================================================
-- Function: validate_project_status
-- =============================================================================
--   To Validate Project Status and return TRUE/FALSE as Success/Failure
-- =============================================================================
--  Input Parameters :
--    p_project_status : Project Status
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   FUNCTION validate_project_status (
      p_errbuf           OUT      VARCHAR2,
      p_retcode          OUT      NUMBER,
      p_project_status   IN       VARCHAR2
   )
      RETURN BOOLEAN
   IS
   BEGIN
      DEBUG ('Function: validate_project_status');

      IF p_project_status IS NULL
      THEN
         p_errbuf := 'Error : Project Status cannot be NULL';
         p_retcode := g_warning;
         DEBUG (p_errbuf);
         RETURN FALSE;
      ELSE
         IF    p_project_status = g_prj_sts_approved
            OR p_project_status = g_prj_sts_unapproved
      --OR p_project_status = g_prj_sts_etnunapproved
         THEN
            RETURN TRUE;
         ELSE
            p_errbuf :=
                  'Error : Project Statuses other than '
               || g_prj_sts_approved
               || '/'
               || g_prj_sts_unapproved
               || ' are not allowed';
            p_retcode := g_warning;
            DEBUG (p_errbuf);
            RETURN FALSE;
         END IF;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR (   'Exception in Function validate_project_status. '
                    || SQLERRM,
                    1,
                    2000
                   );
         p_retcode := g_error;
         DEBUG (p_errbuf);
         RETURN FALSE;
   END validate_project_status;

-- =============================================================================
-- Procedure: load_data
-- =============================================================================
--   Executed in 'LOAD-DATA' Run mode to transfer validated records in Extraction
--   table to Staging table
-- =============================================================================
--  Input Parameters :
--    No Input Parameters
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   PROCEDURE load_data (p_errbuf OUT VARCHAR2, p_retcode OUT NUMBER)
   IS
      l_error_message   VARCHAR2 (2000);
      l_blk_err_var1    VARCHAR2 (2000);
      l_stg_count       NUMBER          := 0;
      l_err_count       NUMBER          := 0;
    l_tot_count       NUMBER          := 0;

      /* Cursor to fetch data from extraction table to insert into Staging table */
      CURSOR stg_cur
      IS
         SELECT xoper.interface_txn_id,
                xoper.batch_id,
                xoper.run_sequence_id,
                xoper.leg_project_number,
                xoper.leg_project_name,
                xoper.leg_project_type,
                xoper.leg_organization,
                xoper.leg_operating_unit,
                xoper.leg_project_long_name,
                xoper.leg_prj_trans_duration_from,
                xoper.leg_prj_trans_duration_to,
                xoper.leg_prj_description,
                xoper.leg_template_name,
                xoper.leg_template_flag,
                xoper.leg_prj_status,
                xoper.leg_public_sector,
                xoper.leg_attribute_category,
                xoper.leg_attribute1,
                xoper.leg_attribute2,
                xoper.leg_attribute3,
                xoper.leg_attribute4,
                xoper.leg_attribute5,
                xoper.leg_attribute6,
                xoper.leg_attribute7,
                xoper.leg_attribute8,
                xoper.leg_attribute9,
                xoper.leg_attribute10,
                xoper.leg_attribute11,
                xoper.leg_attribute12,
                xoper.leg_attribute13,
                xoper.leg_attribute14,
                xoper.leg_attribute15,
                NULL AS org_id,
                NULL AS carrying_out_organization_id,
                NULL AS project_template_id,
                g_flag_ntprocessed AS process_flag,
                NULL AS ERROR_TYPE,
                xoper.attribute_category,
                xoper.attribute1,
                xoper.attribute2,
                xoper.attribute3,
                xoper.attribute4,
                xoper.attribute5,
                xoper.attribute6,
                xoper.attribute7,
                xoper.attribute8,
                xoper.attribute9,
                xoper.attribute10,
                xoper.attribute11,
                xoper.attribute12,
                xoper.attribute13,
                xoper.attribute14,
                xoper.attribute15,
                xoper.leg_source_system,
                xoper.leg_request_id,
                xoper.leg_seq_num,
                xoper.leg_process_flag
           FROM xxpa_open_projects_ext_r12 xoper
          WHERE xoper.leg_process_flag = g_flag_validated
            AND NOT EXISTS (
                          SELECT 1
                            FROM xxpa_open_projects_stg xops
                           WHERE xops.interface_txn_id =
                                                        xoper.interface_txn_id);

      TYPE stg_cur_t IS TABLE OF stg_cur%ROWTYPE
         INDEX BY BINARY_INTEGER;

      l_stg_cur_t       stg_cur_t;
   BEGIN
      p_errbuf := NULL;
      p_retcode := g_normal;
      fnd_file.put_line
         (fnd_file.output,
          '==================================================================================================='
         );
      fnd_file.put_line
           (fnd_file.output,
            'Program Name : Eaton Unify Projects and Tasks Conversion Program'
           );
      fnd_file.put_line (fnd_file.output,
                         'Request ID   : ' || TO_CHAR (g_request_id)
                        );
      fnd_file.put_line (fnd_file.output,
                            'Report Date  : '
                         || TO_CHAR (SYSDATE, 'DD-MON-RRRR HH:MI:SS AM')
                        );
      fnd_file.put_line
         (fnd_file.output,
          '-------------------------------------------------------------------------------------------------'
         );
      fnd_file.put_line (fnd_file.output, 'Parameters  : ');
      fnd_file.put_line (fnd_file.output,
                         '---------------------------------------------'
                        );
      fnd_file.put_line (fnd_file.output,
                         'Run Mode              : ' || g_run_mode
                        );
      fnd_file.put_line (fnd_file.output,
                         'Process records     : ' || g_process_records
                        );
      fnd_file.put_line
         (fnd_file.output,
          '==================================================================================================='
         );
      fnd_file.put_line (fnd_file.output,
                         'Statistics (' || g_run_mode || '):');

      OPEN stg_cur;

      LOOP
         FETCH stg_cur
         BULK COLLECT INTO l_stg_cur_t LIMIT g_limit;

         IF l_stg_cur_t.COUNT > 0
         THEN
            BEGIN
               FORALL indx IN 1 .. l_stg_cur_t.COUNT SAVE EXCEPTIONS
                  INSERT INTO xxpa_open_projects_stg
                              (interface_txn_id,
                               batch_id,
                               run_sequence_id,
                               leg_project_number,
                               leg_project_name,
                               leg_project_type,
                               leg_organization,
                               leg_operating_unit,
                               leg_project_long_name,
                               leg_prj_trans_duration_from,
                               leg_prj_trans_duration_to,
                               leg_prj_description,
                               leg_template_name,
                               leg_template_flag,
                               leg_prj_status,
                               leg_public_sector,
                               leg_attribute_category,
                               leg_attribute1,
                               leg_attribute2,
                               leg_attribute3,
                               leg_attribute4,
                               leg_attribute5,
                               leg_attribute6,
                               leg_attribute7,
                               leg_attribute8,
                               leg_attribute9,
                               leg_attribute10,
                               leg_attribute11,
                               leg_attribute12,
                               leg_attribute13,
                               leg_attribute14,
                               leg_attribute15,
                               org_id,
                               carrying_out_organization_id,
                               project_template_id,
                               creation_date, created_by, last_updated_date,
                               last_updated_by, last_update_login,
                               program_application_id, program_id,
                               program_update_date, request_id,
                               process_flag,
                               ERROR_TYPE,
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
                               leg_source_system,
                               leg_request_id,
                               leg_seq_num,
                               leg_process_flag
                              )
                       VALUES (l_stg_cur_t (indx).interface_txn_id,
                               l_stg_cur_t (indx).batch_id,
                               l_stg_cur_t (indx).run_sequence_id,
                               l_stg_cur_t (indx).leg_project_number,
                               l_stg_cur_t (indx).leg_project_name,
                               l_stg_cur_t (indx).leg_project_type,
                               l_stg_cur_t (indx).leg_organization,
                               l_stg_cur_t (indx).leg_operating_unit,
                               l_stg_cur_t (indx).leg_project_long_name,
                               l_stg_cur_t (indx).leg_prj_trans_duration_from,
                               l_stg_cur_t (indx).leg_prj_trans_duration_to,
                               l_stg_cur_t (indx).leg_prj_description,
                               l_stg_cur_t (indx).leg_template_name,
                               l_stg_cur_t (indx).leg_template_flag,
                               l_stg_cur_t (indx).leg_prj_status,
                               l_stg_cur_t (indx).leg_public_sector,
                               l_stg_cur_t (indx).leg_attribute_category,
                               l_stg_cur_t (indx).leg_attribute1,
                               l_stg_cur_t (indx).leg_attribute2,
                               l_stg_cur_t (indx).leg_attribute3,
                               l_stg_cur_t (indx).leg_attribute4,
                               l_stg_cur_t (indx).leg_attribute5,
                               l_stg_cur_t (indx).leg_attribute6,
                               l_stg_cur_t (indx).leg_attribute7,
                               l_stg_cur_t (indx).leg_attribute8,
                               l_stg_cur_t (indx).leg_attribute9,
                               l_stg_cur_t (indx).leg_attribute10,
                               l_stg_cur_t (indx).leg_attribute11,
                               l_stg_cur_t (indx).leg_attribute12,
                               l_stg_cur_t (indx).leg_attribute13,
                               l_stg_cur_t (indx).leg_attribute14,
                               l_stg_cur_t (indx).leg_attribute15,
                               l_stg_cur_t (indx).org_id,
                               l_stg_cur_t (indx).carrying_out_organization_id,
                               l_stg_cur_t (indx).project_template_id,
                               SYSDATE, g_user_id, SYSDATE,
                               g_user_id, g_login_id,
                               g_prog_appl_id, g_program_id,
                               SYSDATE, g_request_id,
                               l_stg_cur_t (indx).process_flag,
                               l_stg_cur_t (indx).ERROR_TYPE,
                               l_stg_cur_t (indx).attribute_category,
                               l_stg_cur_t (indx).attribute1,
                               l_stg_cur_t (indx).attribute2,
                               l_stg_cur_t (indx).attribute3,
                               l_stg_cur_t (indx).attribute4,
                               l_stg_cur_t (indx).attribute5,
                               l_stg_cur_t (indx).attribute6,
                               l_stg_cur_t (indx).attribute7,
                               l_stg_cur_t (indx).attribute8,
                               l_stg_cur_t (indx).attribute9,
                               l_stg_cur_t (indx).attribute10,
                               l_stg_cur_t (indx).attribute11,
                               l_stg_cur_t (indx).attribute12,
                               l_stg_cur_t (indx).attribute13,
                               l_stg_cur_t (indx).attribute14,
                               l_stg_cur_t (indx).attribute15,
                               l_stg_cur_t (indx).leg_source_system,
                               l_stg_cur_t (indx).leg_request_id,
                               l_stg_cur_t (indx).leg_seq_num,
                               l_stg_cur_t (indx).leg_process_flag
                              );
            EXCEPTION
               WHEN g_bulk_exception
               THEN
                  DEBUG ('Bulk Exception Count '
                         || SQL%BULK_EXCEPTIONS.COUNT
                        );

                  FOR exep_indx IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
                  LOOP
                     l_blk_err_var1 :=
                        l_stg_cur_t
                               (SQL%BULK_EXCEPTIONS (exep_indx).ERROR_INDEX).interface_txn_id;
                     l_error_message :=
                        SUBSTR
                           (   'Bulk Exception occured while Inserting extracted project data into conversion staging. '
                            || SQLERRM
                                  (  -1
                                   * (SQL%BULK_EXCEPTIONS (exep_indx).ERROR_CODE
                                     )
                                  ),
                            1,
                            2000
                           );
                     DEBUG (l_error_message);
                  END LOOP;
            END;

            FOR i IN 1 .. l_stg_cur_t.COUNT
            LOOP
               l_stg_count := l_stg_count + SQL%BULK_ROWCOUNT (i);
            END LOOP;
         END IF;

         COMMIT;
         EXIT WHEN stg_cur%NOTFOUND;
      END LOOP;

      CLOSE stg_cur;

      DEBUG ('Records loaded to Staging Table           ' || l_stg_count);

      /* Updating Extraction table for records successfully loaded into Staging table */
      BEGIN
         UPDATE xxpa_open_projects_ext_r12 xoper
            SET xoper.leg_process_flag = g_flag_processed,
                xoper.last_updated_date = SYSDATE,
                xoper.last_updated_by = g_user_id,
                xoper.last_update_login = g_login_id,
                xoper.request_id = g_request_id                          --1.1
          WHERE xoper.leg_process_flag = g_flag_validated
            AND EXISTS (SELECT 1
                          FROM xxpa_open_projects_stg xops
                         WHERE xops.interface_txn_id = xoper.interface_txn_id);

         DEBUG ('Extraction Table records marked Processed ' || SQL%ROWCOUNT);
      EXCEPTION
         WHEN OTHERS
         THEN
            l_error_message :=
               SUBSTR
                  (   'Exception in Procedure load_data while updating Extraction table records as P. SQLCODE '
                   || SQLERRM,
                   1,
                   2000
                  );
            DEBUG (l_error_message);
      END;

      /* Updating Extraction table for records failed while loading into Staging table */
      BEGIN
         UPDATE xxpa_open_projects_ext_r12 xoper
            SET xoper.leg_process_flag = g_flag_error,
                xoper.last_updated_date = SYSDATE,
                xoper.last_updated_by = g_user_id,
                xoper.last_update_login = g_login_id,
                xoper.request_id = g_request_id
          WHERE xoper.leg_process_flag = g_flag_validated
            AND NOT EXISTS (
                          SELECT 1
                            FROM xxpa_open_projects_stg xops
                           WHERE xops.interface_txn_id =
                                                        xoper.interface_txn_id);

         l_err_count := SQL%ROWCOUNT;
         DEBUG ('Extraction Table records marked Error ' || SQL%ROWCOUNT);
      EXCEPTION
         WHEN OTHERS
         THEN
            l_error_message :=
               SUBSTR
                  (   'Exception in Procedure load_data while updating Extraction table records as E. SQLCODE '
                   || SQLERRM,
                   1,
                   2000
                  );
            DEBUG (l_error_message);
      END;

    l_tot_count := l_stg_count +  l_err_count;

      fnd_file.put_line (fnd_file.output,
                            'Records Submitted       : '
                         || l_tot_count
                        );
      fnd_file.put_line (fnd_file.output,
                         'Records Extracted       : ' || l_stg_count
                        );
      fnd_file.put_line (fnd_file.output,
                         'Records Erred         : ' || l_err_count
                        );
      fnd_file.put_line (fnd_file.output, CHR (10));
      fnd_file.put_line
         (fnd_file.output,
          '==================================================================================================='
         );
   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR ('Exception in Procedure load_data. ' || SQLERRM, 1, 2000);
         p_retcode := g_warning;
         DEBUG (p_errbuf);
     fnd_file.put_line (fnd_file.log, 'Error : Back trace : '
                            || DBMS_UTILITY.format_error_backtrace);
   END load_data;

-- =============================================================================
-- Procedure: pre_validate
-- =============================================================================
--   To validate the Lookup Setups
-- =============================================================================
--  Input Parameters :
--    No Input Parameters
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   PROCEDURE pre_validate (p_errbuf OUT VARCHAR2, p_retcode OUT NUMBER)
   IS
      l_lookup_count     NUMBER          := NULL;
      l_template_count   NUMBER          := NULL;
      l_error_message    VARCHAR2 (2000) := NULL;
   BEGIN
      p_errbuf := NULL;
      p_retcode := g_normal;

      /* Checking for Lookup Setup */
      SELECT COUNT (*)
        INTO l_lookup_count
        FROM fnd_lookup_types flt
       WHERE flt.lookup_type = g_common_ou_map;

      DEBUG ('l_lookup_count ' || l_lookup_count);

      IF l_lookup_count <> 1
      THEN
         l_error_message :=
               'ERROR : Organization Mapping common Lookup :'
            || g_common_ou_map
            || ' is not setup in System';
         p_retcode := g_warning;
         p_errbuf := l_error_message;
      ELSIF l_lookup_count = 1
      THEN
         /* Checking if the Lookup has values pupulated */
         SELECT COUNT (*)
           INTO l_lookup_count
           FROM fnd_lookup_values flv
          WHERE flv.lookup_type = g_common_ou_map;

         DEBUG ('l_lookup_count ' || l_lookup_count);

         IF l_lookup_count < 1
         THEN
            l_error_message :=
                  'ERROR : Organization Mapping common Lookup '
               || g_common_ou_map
               || ' values not setup';
            p_retcode := g_warning;
            p_errbuf := l_error_message;
         END IF;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR ('Exception in Procedure pre_validate. ' || SQLERRM,
                    1,
                    2000
                   );
         p_retcode := g_error;
         DEBUG (p_errbuf);
   END pre_validate;

-- =============================================================================
-- Procedure: derive_Operating_Unit
-- =============================================================================
--   Derive the  Operating Unit Id and update in Staging table
-- =============================================================================
--  Input Parameters :
--    No Input Parameters
--    Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   PROCEDURE derive_Operating_Unit (p_errbuf OUT VARCHAR2, p_retcode OUT NUMBER)
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_error_tab_type   xxetn_common_error_pkg.g_source_tab_type;
      l_log_ret_sts      VARCHAR2 (2000);
      l_log_err_msg      VARCHAR2 (2000);
      l_error_message    VARCHAR2 (2000) := NULL;

    l_oper_unit  xxetn_map_unit.operating_unit%TYPE;
      l_rec      xxetn_map_util.g_input_rec;
    l_org_id    NUMBER;

    CURSOR derive_ou_cur
    IS
      SELECT distinct substr(leg_project_number,1,4) plant
        FROM xxpa_open_projects_stg
     WHERE process_flag =g_flag_ntprocessed
       AND batch_id = g_new_batch_id
         AND run_sequence_id = g_run_sequence_id;


   BEGIN
      p_errbuf := NULL;
      p_retcode := g_normal;
      FOR cur_derive_ou in derive_ou_cur
    LOOP

      l_rec.site      := cur_derive_ou.plant;
    l_oper_unit     := xxetn_map_util.get_value (l_rec).operating_unit;

    IF l_oper_unit IS NOT NULL
    THEN
      BEGIN
        SELECT organization_id
          INTO l_org_id
          FROM hr_all_organization_units
         WHERE name = l_oper_unit;

         UPDATE xxpa_open_projects_stg
            SET org_id = l_org_id
          WHERE substr(leg_project_number,1,4)=cur_derive_ou.plant;

      EXCEPTION
        WHEN OTHERS THEN
           l_error_message :=
           SUBSTR
            (   'Exception in Procedure derive_Operating_Unit while Deriveing OU from ETN_MAP_UNIT and updating Staging table records. SQL ERROR: '
             || SQLERRM,
             1,
             2000
            );
          DEBUG (l_error_message);
      END;

    END IF;

    END LOOP;
    /* Logging Error for records for which Operating Unit Id not derived */
      SELECT g_source_table                                     --source_table
             ,xops.interface_txn_id                      --interface_staging_id
             ,NULL                                            --source_keyname1
             ,NULL                                           --source_keyvalue1
             ,NULL                                            --source_keyname2
             ,NULL                                           --source_keyvalue2
             ,NULL                                            --source_keyname3
             ,NULL                                           --source_keyvalue3
             ,NULL                                            --source_keyname4
             ,NULL                                           --source_keyvalue4
             ,NULL                                            --source_keyname5
             ,NULL                                           --source_keyvalue5
             ,'ORG_ID'                                     --source_column_name
             ,org_id                                      --source_column_value
             ,g_err_val                                            --error_type
             ,'ETN_PA_OPERATING_UNIT_DERIVATION'                   --error_code
              ,'R12 Operating Unit against Legacy Operating Unit '
             || leg_operating_unit
             || ' could not be derived, verify Cross reference mapping xxetn_map_util'
                                                                                         --error_message
             ,NULL                                                   --severity
             ,NULL                                          --proposed_solution
             ,NULL
             ,NULL --AS PER CHANGES DONE BY SAGAR
      BULK COLLECT INTO l_error_tab_type
        FROM xxpa_open_projects_stg xops
       WHERE xops.batch_id = g_new_batch_id
         AND xops.run_sequence_id = g_run_sequence_id
         AND xops.org_id IS NULL
         AND NVL(xops.process_flag,'$') <> DECODE(process_flag,NULL,'%',g_flag_completed) ; ----added as per version 1.3

    DEBUG ('04. l_error_tab_type.COUNT ' || l_error_tab_type.COUNT);

      IF l_error_tab_type.COUNT > 0
      THEN
         log_error (p_source_tab_type      => l_error_tab_type,
                    pov_return_status      => l_log_ret_sts,
                    pov_error_message      => l_log_err_msg
                   );
         l_error_tab_type.DELETE;
      END IF;


      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR ('Exception in Procedure derive_Operating_Unit. ' || SQLERRM,
                    1,
                    2000
                   );
         p_retcode := g_error;
         DEBUG (p_errbuf);
         ROLLBACK;
   END derive_Operating_Unit;


-- =============================================================================
-- Procedure: derive_organization
-- =============================================================================
--   Derive the Project Org Id, Operating Unit Id and update in Staging table
-- =============================================================================
--  Input Parameters :
--    No Input Parameters
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   PROCEDURE derive_organization (p_errbuf OUT VARCHAR2, p_retcode OUT NUMBER)
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_error_tab_type   xxetn_common_error_pkg.g_source_tab_type;
      l_log_ret_sts      VARCHAR2 (2000);
      l_log_err_msg      VARCHAR2 (2000);
      l_error_message    VARCHAR2 (2000) := NULL;



   BEGIN
      p_errbuf := NULL;
      p_retcode := g_normal;

      /* Logging Error for records for which Legacy Project Organization field is NULL */
      SELECT g_source_table                                     --source_table
             ,xops.interface_txn_id                      --interface_staging_id
             ,NULL                                            --source_keyname1
             ,NULL                                           --source_keyvalue1
             ,NULL                                            --source_keyname2
             ,NULL                                           --source_keyvalue2
             ,NULL                                            --source_keyname3
             ,NULL                                           --source_keyvalue3
             ,NULL                                            --source_keyname4
             ,NULL                                           --source_keyvalue4
             ,NULL                                            --source_keyname5
             ,NULL                                           --source_keyvalue5
             ,'LEG_ORGANIZATION'                           --source_column_name
             ,leg_organization                            --source_column_value
             ,g_err_val                                            --error_type
             ,'ETN_PA_PROJECT_LEG_ORGANIZATION'                    --error_code
             ,'Error : Legacy Project Organization field cannot be NULL'
                                                                       --error_message
             ,NULL                                                   --severity
             ,NULL                                          --proposed_solution
             ,NULL
              ,NULL --AS PER CHANGES DONE BY SAGAR
      BULK COLLECT INTO l_error_tab_type
        FROM xxpa_open_projects_stg xops
       WHERE xops.batch_id = g_new_batch_id
         AND xops.run_sequence_id = g_run_sequence_id
         AND xops.leg_organization IS NULL
         AND NVL(xops.process_flag,'$') <> DECODE(process_flag,NULL,'%',g_flag_completed) ; ----added as per version 1.3

      DEBUG ('01. l_error_tab_type.COUNT ' || l_error_tab_type.COUNT);

      IF l_error_tab_type.COUNT > 0
      THEN
         log_error (p_source_tab_type      => l_error_tab_type,
                    pov_return_status      => l_log_ret_sts,
                    pov_error_message      => l_log_err_msg
                   );
         l_error_tab_type.DELETE;
      END IF;


      /* Deriving Project Org Id and updating in Staging Table */
      BEGIN

        UPDATE xxpa_open_projects_stg xops
         SET xops.carrying_out_organization_id =
                                         (SELECT organization_id
                                            FROM hr_all_organization_units
                                         --  WHERE NAME = xops.leg_organization) changed by Harjinder Singh commented to add new condition as per Nitin Mail  1.1
                                             WHERE SUBSTR(NAME, 1, 7) = SUBSTR(xops.leg_organization, 1, 7))

             /*xops.org_id = (SELECT organization_id
                            FROM   hr_operating_units
                            WHERE name =
                      (SELECT NAME
                              FROM fnd_lookup_values flv,
                                   hr_operating_units hou
                             WHERE flv.meaning = xops.leg_operating_unit
                               AND flv.lookup_type = g_common_ou_map
                               AND flv.LANGUAGE = USERENV ('LANG')
                               AND flv.enabled_flag = g_flag_yes
                               AND TRUNC (SYSDATE)
                                      BETWEEN NVL (start_date_active, SYSDATE)
                                          AND NVL (end_date_active,
                                                   SYSDATE + 1
                                                  )
                               AND flv.description = hou.NAME))
       */
--             xops.project_template_id = g_project_template_id
        WHERE xops.batch_id = g_new_batch_id
         AND xops.run_sequence_id = g_run_sequence_id
         AND xops.process_flag = g_flag_ntprocessed
         AND xops.leg_organization IS NOT NULL;
        -- AND xops.leg_operating_unit IS NOT NULL;

        DEBUG ('No. of records updated ' || SQL%ROWCOUNT);

      EXCEPTION
         WHEN OTHERS
         THEN
            l_error_message :=
               SUBSTR
                  (   'Exception in Procedure derive_organization while updating Staging table records. SQLCODE: '
                   || SQLERRM,
                   1,
                   2000
                  );
            DEBUG (l_error_message);
      END;


      /* Logging Error for records for which Project org Id not derived */
      SELECT g_source_table                                     --source_table
             ,xops.interface_txn_id                      --interface_staging_id
             ,NULL                                            --source_keyname1
             ,NULL                                           --source_keyvalue1
             ,NULL                                            --source_keyname2
             ,NULL                                           --source_keyvalue2
             ,NULL                                            --source_keyname3
             ,NULL                                           --source_keyvalue3
             ,NULL                                            --source_keyname4
             ,NULL                                           --source_keyvalue4
             ,NULL                                            --source_keyname5
             ,NULL                                           --source_keyvalue5
             ,'CARRYING_OUT_ORGANIZATION_ID'               --source_column_name
             ,carrying_out_organization_id                --source_column_value
             ,g_err_val                                            --error_type
             ,'ETN_PA_PROJECT_ORG_DERIVATION'                      --error_code
              ,'R12 Project Org against Legacy Org Name '
             || leg_organization
             || ' could not be derived, verify Project Org Setup'
                                                                 --error_message
             ,NULL                                                   --severity
             ,NULL                                          --proposed_solution
             ,NULL
              ,NULL --AS PER CHANGES DONE BY SAGAR
      BULK COLLECT INTO l_error_tab_type
        FROM xxpa_open_projects_stg xops
       WHERE xops.batch_id = g_new_batch_id
         AND xops.run_sequence_id = g_run_sequence_id
         AND xops.leg_organization IS NOT NULL
         --AND xops.leg_operating_unit IS NOT NULL
         AND xops.carrying_out_organization_id IS NULL
          AND NVL(xops.process_flag,'$') <> DECODE(process_flag,NULL,'%',g_flag_completed) ; ----added as per version 1.3

      DEBUG ('03. l_error_tab_type.COUNT ' || l_error_tab_type.COUNT);

      IF l_error_tab_type.COUNT > 0
      THEN
         log_error (p_source_tab_type      => l_error_tab_type,
                    pov_return_status      => l_log_ret_sts,
                    pov_error_message      => l_log_err_msg
                   );
         l_error_tab_type.DELETE;
      END IF;


      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR ('Exception in Procedure derive_organization. ' || SQLERRM,
                    1,
                    2000
                   );
         p_retcode := g_error;
         DEBUG (p_errbuf);
         ROLLBACK;
   END derive_organization;

-- =============================================================================
-- Procedure: derive_project_template
-- =============================================================================
--   Derive the Project Template Id based on the Template Name
-- =============================================================================
--  Input Parameters :
--    No Input Parameters
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   PROCEDURE derive_project_template (
      p_errbuf    OUT   VARCHAR2,
      p_retcode   OUT   NUMBER
   )
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_error_tab_type   xxetn_common_error_pkg.g_source_tab_type;
      l_log_ret_sts      VARCHAR2 (2000);
      L_LOG_ERR_MSG      VARCHAR2 (2000);
      l_error_message    VARCHAR2 (2000);
   BEGIN
      p_errbuf := NULL;
      p_retcode := g_normal;

      /* Deriving Project Template Id and updating into Staging table  */
      BEGIN
        UPDATE xxpa_open_projects_stg xops
         SET xops.project_template_id =
             --Commented for  v1.3 START
               /* (SELECT ppa.project_id
                   FROM pa_projects_all ppa
                  WHERE ppa.NAME LIKE 'T,%' || g_project_template
                    AND ppa.template_flag = g_flag_yes
                    AND ppa.org_id = xops.org_id
                    AND TRUNC (SYSDATE) BETWEEN ppa.start_date
                                            AND NVL (ppa.completion_date,
                                                     SYSDATE + 1
                                                    ))*/
             --Commented for  v1.3 END
             --Added for v1.3 START
               (SELECT ppa.project_id
                   FROM pa_projects_all ppa
                  WHERE ppa.NAME LIKE 'T, '|| (SELECT SUBSTR(NAME, 4, 4)
                                               FROM apps.hr_all_organization_units
                                               WHERE organization_id = xops.carrying_out_organization_id)
                                           ||' %' || g_project_template
                    AND ppa.template_flag = g_flag_yes
                    AND ppa.org_id = xops.org_id
                    AND TRUNC (SYSDATE) BETWEEN ppa.start_date
                                            AND NVL (ppa.completion_date,
                                                     SYSDATE + 1
                                                    ))                                     
             --Added for v1.3 END
                                                          
        WHERE xops.batch_id = g_new_batch_id
         AND xops.run_sequence_id = g_run_sequence_id
         AND xops.org_id IS NOT NULL
          AND NVL(xops.process_flag,'$') <> DECODE(process_flag,NULL,'%',g_flag_completed) ; ----added as per version 1.3

        DEBUG ('No. of records updated ' || SQL%ROWCOUNT);

      EXCEPTION
         WHEN OTHERS
         THEN
            l_error_message :=
               SUBSTR
                  (   'Exception in Procedure derive_project_template while updating Staging table records. SQLCODE: '
                   || SQLERRM,
                   1,
                   2000
                  );
            DEBUG (l_error_message);
      END;


      /* Logging Error for records for which Project Template Id not derived */
      SELECT g_source_table                                     --source_table
             ,xops.interface_txn_id                      --interface_staging_id
             ,NULL                                            --source_keyname1
             ,NULL                                           --source_keyvalue1
             ,NULL                                            --source_keyname2
             ,NULL                                           --source_keyvalue2
             ,NULL                                            --source_keyname3
             ,NULL                                           --source_keyvalue3
             ,NULL                                            --source_keyname4
             ,NULL                                           --source_keyvalue4
             ,NULL                                            --source_keyname5
             ,NULL                                           --source_keyvalue5
             ,'PROJECT_TEMPLATE_ID'                        --source_column_name
             ,project_template_id                         --source_column_value
             ,g_err_val                                            --error_type
             ,'ETN_PA_PROJECT_TMPLT_DERIVATION'                    --error_code
              ,'R12 Project Template Id Could not be derived for Operating Unit '
             || leg_operating_unit
                                  --error_message
             ,NULL                                                   --severity
             ,NULL                                          --proposed_solution
             ,NULL
              ,NULL --AS PER CHANGES DONE BY SAGAR
      BULK COLLECT INTO l_error_tab_type
        FROM xxpa_open_projects_stg xops
       WHERE xops.batch_id = g_new_batch_id
         AND xops.run_sequence_id = g_run_sequence_id
         AND xops.org_id IS NOT NULL
         AND xops.project_template_id IS NULL
         AND xops.process_flag <> g_flag_completed ; ----added as per version 1.3

      DEBUG ('l_error_tab_type.COUNT ' || l_error_tab_type.COUNT);

      IF l_error_tab_type.COUNT > 0
      THEN
         log_error (p_source_tab_type      => l_error_tab_type,
                    pov_return_status      => l_log_ret_sts,
                    pov_error_message      => l_log_err_msg
                   );
         l_error_tab_type.DELETE;
      END IF;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         p_errbuf :=
            SUBSTR (   'Exception in Procedure derive_project_template. '
                    || SQLERRM,
                    1,
                    2000
                   );
         p_retcode := g_error;
         DEBUG (p_errbuf);
   END derive_project_template;

-- =============================================================================
-- Procedure: assign_batch_id
-- =============================================================================
--   To assign Batch Id and Run Sequence Id
-- =============================================================================
--  Input Parameters :
--    No Input Parameters
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   PROCEDURE assign_batch_id (p_errbuf OUT VARCHAR2, p_retcode OUT NUMBER)
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_log_ret_sts     VARCHAR2 (2000);
      l_log_err_msg     VARCHAR2 (2000);
      l_error_message   VARCHAR2 (2000);
      vl_update_count   NUMBER;
   BEGIN
      p_errbuf := NULL;
      p_retcode := g_normal;

--
      /* logic to generate Batch Id and Run Sequence Id */
      IF g_batch_id IS NULL
      THEN
         /* Generate both Batch Id and Run Sequence Id (and assiging to global placeholders) when Batch Id is passed as null in program parameter */
         g_new_batch_id := xxetn_batches_s.NEXTVAL;
         g_run_sequence_id := xxetn_run_sequences_s.NEXTVAL;
      ELSE
         /* Generate only Run Sequence Id (and assiging to global placeholders) when Batch Id is passed with a value in program parameter */
         g_new_batch_id := g_batch_id;
         g_run_sequence_id := xxetn_run_sequences_s.NEXTVAL;
      END IF;
      fnd_file.put_line
            (fnd_file.output,
             'g_new_batch_id      ' || g_new_batch_id ||'--'||
             'g_batch_id      ' || g_batch_id
            );

      DEBUG ('g_new_batch_id      ' || g_new_batch_id);
      DEBUG ('g_run_sequence_id   ' || g_run_sequence_id);


      /* Updating Batch Id and Run Sequence Id in staging table based on the program Run Mode */

      BEGIN

        IF g_run_mode = g_run_mode_conversion
        THEN
           UPDATE xxpa_open_projects_stg
              SET run_sequence_id = g_run_sequence_id,
                  last_updated_date = SYSDATE,
                  last_updated_by = g_user_id,
                  last_update_login = g_login_id,
                  request_id = g_request_id
            WHERE batch_id = g_new_batch_id AND process_flag = g_flag_validated;

           DEBUG
                (   'Program in CONVERSION mode; Staging table records updated '
                 || SQL%ROWCOUNT
                );
        ELSIF g_run_mode = g_run_mode_validate
        THEN
           IF g_batch_id IS NULL
           THEN

              UPDATE xxpa_open_projects_stg
                 SET batch_id = g_new_batch_id,
                     run_sequence_id = g_run_sequence_id,
                     last_updated_date = SYSDATE,
                     last_updated_by = g_user_id,
                     last_update_login = g_login_id,
                     request_id = g_request_id
               WHERE batch_id IS NULL
             AND NVL(process_flag,'$') <> DECODE(process_flag,NULL,'%',g_flag_completed);
            --  AND process_flag IN (g_flag_ntprocessed, g_flag_error, g_flag_validated) ;
              ----added as per version 1.3

              vl_update_count := sql%rowcount;


              DEBUG
                 (   'Program in VALIDATE mode; BatchId is null; Staging table records updated '
                  || SQL%ROWCOUNT
                 );
           ELSIF g_batch_id IS NOT NULL
           THEN
              IF g_process_records = g_process_recs_all
              THEN
                 UPDATE xxpa_open_projects_stg
                    SET process_flag = g_flag_ntprocessed,
                        run_sequence_id = g_run_sequence_id,
                        last_updated_date = SYSDATE,
                        last_updated_by = g_user_id,
                        last_update_login = g_login_id,
                        program_application_id = g_prog_appl_id,
                        program_id = g_program_id,
                        program_update_date = SYSDATE,
                        request_id = g_request_id
                  WHERE batch_id = g_new_batch_id
                    AND process_flag IN (g_flag_ntprocessed, g_flag_error, g_flag_validated)
                     /*AND process_flag <> g_flag_completed */;----added as per version 1.3

                 DEBUG
                    (   g_process_records
                     || ' Program in VALIDATE mode; BatchId is not null; Process Records is '
                     || g_process_records
                     || ' Staging table records updated '
                     || SQL%ROWCOUNT
                    );
              ELSIF g_process_records = g_process_recs_error
              THEN
                 UPDATE xxpa_open_projects_stg
                    SET process_flag = g_flag_ntprocessed,
                        run_sequence_id = g_run_sequence_id,
                        last_updated_date = SYSDATE,
                        last_updated_by = g_user_id,
                        last_update_login = g_login_id,
                        program_application_id = g_prog_appl_id,
                        program_id = g_program_id,
                        program_update_date = SYSDATE,
                        request_id = g_request_id
                  WHERE process_flag = g_flag_error;

                 DEBUG
                    (   g_process_records
                     || ' Program in VALIDATE mode; BatchId is not null; Process Records is '
                     || g_process_records
                     || ' Staging table records updated '
                     || SQL%ROWCOUNT
                    );
              ELSIF g_process_records = g_process_recs_unprocessed
              THEN
                 UPDATE xxpa_open_projects_stg
                    SET run_sequence_id = g_run_sequence_id,
                        last_updated_date = SYSDATE,
                        last_updated_by = g_user_id,
                        last_update_login = g_login_id,
                        program_application_id = g_prog_appl_id,
                        program_id = g_program_id,
                        program_update_date = SYSDATE,
                        request_id = g_request_id
                  WHERE batch_id = g_new_batch_id
                    AND process_flag = g_flag_ntprocessed;

                 DEBUG
                    (   g_process_records
                     || ' Program in VALIDATE mode; BatchId is not null; Process Records is '
                     || g_process_records
                     || ' Staging table records updated '
                     || SQL%ROWCOUNT
                    );
              END IF;
           END IF;
        END IF;

      EXCEPTION
         WHEN OTHERS
         THEN
            l_error_message :=
               SUBSTR
                  (   'Exception in updating Batch Id and Run Sequence Id in staging table. SQLCODE: '
                   || SQLERRM,
                   1,
                   2000
                  );
            DEBUG (l_error_message);
      END;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         l_error_message :=
            SUBSTR (   'Exception in Procedure assign_batch_id. SQLCODE '
                    || SQLCODE
                    || DBMS_UTILITY.format_error_stack
                    || DBMS_UTILITY.format_error_backtrace,
                    1,
                    2000
                   );
         p_errbuf := l_error_message;
         p_retcode := g_error;
         DEBUG (l_error_message);
         ROLLBACK;
   END assign_batch_id;

-- =============================================================================
-- Procedure: validate_project
-- =============================================================================
--   To Validate Project Data
-- =============================================================================
--  Input Parameters :
--    No Input Parameters
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   PROCEDURE validate_project (p_errbuf OUT VARCHAR2, p_retcode OUT NUMBER)
   IS
      l_error_tab_type        xxetn_common_error_pkg.g_source_tab_type;
      l_blk_error_tab_type    xxetn_common_error_pkg.g_source_tab_type;
      l_blk_err_var1          VARCHAR2 (2000);
      l_error_code   CONSTANT VARCHAR2 (25)    := 'ETN_PA_PROJECT_VALIDATION';
      l_error_count           NUMBER                                   := 0;
      l_error_tab_count       NUMBER                                   := 0;
      l_error_message         VARCHAR2 (2000);
      l_log_ret_sts           VARCHAR2 (2000);
      l_log_err_msg           VARCHAR2 (2000);
      l_errbuf                VARCHAR2 (2000)                         := NULL;
      l_retcode               NUMBER                                  := NULL;
      l_loop_count            NUMBER                                   := 0;
      l_validation_flag       BOOLEAN;
      l_org_template_null_flag VARCHAR2(1);

      /* Cursor to fetch records from Staging table eligible for validation */
      CURSOR projects_cur
      IS
         SELECT   xops.*
             FROM xxpa_open_projects_stg xops
            WHERE xops.batch_id = g_new_batch_id
              AND xops.run_sequence_id = g_run_sequence_id
              AND NVL(xops.process_flag,'$') <> DECODE(process_flag,NULL,'%',g_flag_completed)  ----added as per version 1.3
         ORDER BY leg_project_number;

      TYPE projects_cur_t IS TABLE OF projects_cur%ROWTYPE
         INDEX BY BINARY_INTEGER;

      l_projects_cur_t        projects_cur_t;
   BEGIN
      FOR projects_stg_indx IN projects_cur
      LOOP
         l_errbuf := NULL;
         l_org_template_null_flag := 'N';
         l_retcode := g_normal;
         l_error_count := 0;
         l_loop_count := l_loop_count + 1;
         /* Marking record as validated */
         l_projects_cur_t (l_loop_count).interface_txn_id :=
                                           projects_stg_indx.interface_txn_id;
         l_projects_cur_t (l_loop_count).process_flag := g_flag_validated;
         l_projects_cur_t (l_loop_count).ERROR_TYPE := NULL;
         l_validation_flag :=
            check_duplicate_project_num
                                       (l_errbuf,
                                        l_retcode,
                                        projects_stg_indx.leg_project_number,
                                        projects_stg_indx.interface_txn_id
                                       );

         IF l_retcode <> g_normal
         THEN
            l_error_count := l_error_count + 1;
            l_error_tab_count := l_error_tab_count + 1;
            l_error_message := SUBSTR (l_errbuf, 1, 2000);
            l_error_tab_type (l_error_tab_count).source_table :=
                                                               g_source_table;
            l_error_tab_type (l_error_tab_count).interface_staging_id :=
                                           projects_stg_indx.interface_txn_id;
            l_error_tab_type (l_error_tab_count).ERROR_TYPE := g_err_val;
            l_error_tab_type (l_error_tab_count).ERROR_CODE := l_error_code;
            l_error_tab_type (l_error_tab_count).error_message :=
                                                              l_error_message;
            l_error_tab_type (l_error_tab_count).source_column_name :=
                                                         'LEG_PROJECT_NUMBER';
            l_error_tab_type (l_error_tab_count).source_column_value :=
                                         projects_stg_indx.leg_project_number;
         END IF;

         DEBUG (   'check_duplicate_project_num. l_error_count '
                || l_error_count
                || ' l_retcode '
                || l_retcode
                || ' l_errbuf '
                || l_errbuf
               );
         l_validation_flag :=
            check_duplicate_project_name (l_errbuf,
                                          l_retcode,
                                          projects_stg_indx.leg_project_name,
                                          projects_stg_indx.interface_txn_id
                                         );

         IF l_retcode <> g_normal
         THEN
            l_error_count := l_error_count + 1;
            l_error_tab_count := l_error_tab_count + 1;
            l_error_message := SUBSTR (l_errbuf, 1, 2000);
            l_error_tab_type (l_error_tab_count).source_table :=
                                                               g_source_table;
            l_error_tab_type (l_error_tab_count).interface_staging_id :=
                                           projects_stg_indx.interface_txn_id;
            l_error_tab_type (l_error_tab_count).ERROR_TYPE := g_err_val;
            l_error_tab_type (l_error_tab_count).ERROR_CODE := l_error_code;
            l_error_tab_type (l_error_tab_count).error_message :=
                                                              l_error_message;
            l_error_tab_type (l_error_tab_count).source_column_name :=
                                                           'LEG_PROJECT_NAME';
            l_error_tab_type (l_error_tab_count).source_column_value :=
                                           projects_stg_indx.leg_project_name;
         END IF;

         DEBUG (   'check_duplicate_project_name. l_error_count '
                || l_error_count
                || ' l_retcode '
                || l_retcode
                || ' l_errbuf '
                || l_errbuf
               );
         l_validation_flag :=
            check_duplicate_project_lname
                                     (l_errbuf,
                                      l_retcode,
                                      projects_stg_indx.leg_project_long_name,
                                      projects_stg_indx.interface_txn_id
                                     );

         IF l_retcode <> g_normal
         THEN
            l_error_count := l_error_count + 1;
            l_error_tab_count := l_error_tab_count + 1;
            l_error_message := SUBSTR (l_errbuf, 1, 2000);
            l_error_tab_type (l_error_tab_count).source_table :=
                                                               g_source_table;
            l_error_tab_type (l_error_tab_count).interface_staging_id :=
                                           projects_stg_indx.interface_txn_id;
            l_error_tab_type (l_error_tab_count).ERROR_TYPE := g_err_val;
            l_error_tab_type (l_error_tab_count).ERROR_CODE := l_error_code;
            l_error_tab_type (l_error_tab_count).error_message :=
                                                              l_error_message;
            l_error_tab_type (l_error_tab_count).source_column_name :=
                                                      'LEG_PROJECT_LONG_NAME';
            l_error_tab_type (l_error_tab_count).source_column_value :=
                                      projects_stg_indx.leg_project_long_name;
         END IF;

         DEBUG (   'check_duplicate_project_lname. l_error_count '
                || l_error_count
                || ' l_retcode '
                || l_retcode
                || ' l_errbuf '
                || l_errbuf
               );
         -- Check if Project Number exist in System
         l_validation_flag :=
            validate_project_num (l_errbuf,
                                  l_retcode,
                                  projects_stg_indx.leg_project_number
                                 );

         IF l_retcode <> g_normal
         THEN
            l_error_count := l_error_count + 1;
            l_error_tab_count := l_error_tab_count + 1;
            l_error_message := SUBSTR (l_errbuf, 1, 2000);
            l_error_tab_type (l_error_tab_count).source_table :=
                                                               g_source_table;
            l_error_tab_type (l_error_tab_count).interface_staging_id :=
                                           projects_stg_indx.interface_txn_id;
            l_error_tab_type (l_error_tab_count).ERROR_TYPE := g_err_val;
            l_error_tab_type (l_error_tab_count).ERROR_CODE := l_error_code;
            l_error_tab_type (l_error_tab_count).error_message :=
                                                              l_error_message;
            l_error_tab_type (l_error_tab_count).source_column_name :=
                                                         'LEG_PROJECT_NUMBER';
            l_error_tab_type (l_error_tab_count).source_column_value :=
                                         projects_stg_indx.leg_project_number;
         END IF;

         DEBUG (   'validate_project_num. l_error_count '
                || l_error_count
                || ' l_retcode '
                || l_retcode
                || ' l_errbuf '
                || l_errbuf
               );
         --Check if project name exist in system
         l_validation_flag :=
            validate_project_name (l_errbuf,
                                   l_retcode,
                                   projects_stg_indx.leg_project_name
                                  );

         IF l_retcode <> g_normal
         THEN
            l_error_count := l_error_count + 1;
            l_error_tab_count := l_error_tab_count + 1;
            l_error_message := SUBSTR (l_errbuf, 1, 2000);
            l_error_tab_type (l_error_tab_count).source_table :=
                                                               g_source_table;
            l_error_tab_type (l_error_tab_count).interface_staging_id :=
                                           projects_stg_indx.interface_txn_id;
            l_error_tab_type (l_error_tab_count).ERROR_TYPE := g_err_val;
            l_error_tab_type (l_error_tab_count).ERROR_CODE := l_error_code;
            l_error_tab_type (l_error_tab_count).error_message :=
                                                              l_error_message;
            l_error_tab_type (l_error_tab_count).source_column_name :=
                                                           'LEG_PROJECT_NAME';
            l_error_tab_type (l_error_tab_count).source_column_value :=
                                           projects_stg_indx.leg_project_name;
         END IF;

         DEBUG (   'validate_project_name. l_error_count '
                || l_error_count
                || ' l_retcode '
                || l_retcode
                || ' l_errbuf '
                || l_errbuf
               );
         --check if project long name exist in system
         l_validation_flag :=
            validate_project_long_name
                                      (l_errbuf,
                                       l_retcode,
                                       projects_stg_indx.leg_project_long_name
                                      );

         IF l_retcode <> g_normal
         THEN
            l_error_count := l_error_count + 1;
            l_error_tab_count := l_error_tab_count + 1;
            l_error_message := SUBSTR (l_errbuf, 1, 2000);
            l_error_tab_type (l_error_tab_count).source_table :=
                                                               g_source_table;
            l_error_tab_type (l_error_tab_count).interface_staging_id :=
                                           projects_stg_indx.interface_txn_id;
            l_error_tab_type (l_error_tab_count).ERROR_TYPE := g_err_val;
            l_error_tab_type (l_error_tab_count).ERROR_CODE := l_error_code;
            l_error_tab_type (l_error_tab_count).error_message :=
                                                              l_error_message;
            l_error_tab_type (l_error_tab_count).source_column_name :=
                                                      'LEG_PROJECT_LONG_NAME';
            l_error_tab_type (l_error_tab_count).source_column_value :=
                                      projects_stg_indx.leg_project_long_name;
         END IF;

         DEBUG (   'validate_project_long_name. l_error_count '
                || l_error_count
                || ' l_retcode '
                || l_retcode
                || ' l_errbuf '
                || l_errbuf
               );
         l_validation_flag :=
            validate_project_start_date
                                (l_errbuf,
                                 l_retcode,
                                 projects_stg_indx.leg_prj_trans_duration_from
                                );

         IF l_retcode <> g_normal
         THEN
            l_error_count := l_error_count + 1;
            l_error_tab_count := l_error_tab_count + 1;
            l_error_message := SUBSTR (l_errbuf, 1, 2000);
            l_error_tab_type (l_error_tab_count).source_table :=
                                                               g_source_table;
            l_error_tab_type (l_error_tab_count).interface_staging_id :=
                                           projects_stg_indx.interface_txn_id;
            l_error_tab_type (l_error_tab_count).ERROR_TYPE := g_err_val;
            l_error_tab_type (l_error_tab_count).ERROR_CODE := l_error_code;
            l_error_tab_type (l_error_tab_count).error_message :=
                                                              l_error_message;
            l_error_tab_type (l_error_tab_count).source_column_name :=
                                                'LEG_PRJ_TRANS_DURATION_FROM';
            l_error_tab_type (l_error_tab_count).source_column_value :=
                                projects_stg_indx.leg_prj_trans_duration_from;
         END IF;

         DEBUG (   'validate_project_start_date. l_error_count '
                || l_error_count
                || ' l_retcode '
                || l_retcode
                || ' l_errbuf '
                || l_errbuf
               );
         l_validation_flag :=
            validate_project_type (l_errbuf,
                                   l_retcode,
                                   projects_stg_indx.leg_project_type
                                  );
         DEBUG (   'validate_project_type. l_error_count '
                || l_error_count
                || ' l_retcode '
                || l_retcode
                || ' l_errbuf '
                || l_errbuf
               );

         IF l_retcode <> g_normal
         THEN
            l_error_count := l_error_count + 1;
            l_error_tab_count := l_error_tab_count + 1;
            l_error_message := SUBSTR (l_errbuf, 1, 2000);
            l_error_tab_type (l_error_tab_count).source_table :=
                                                               g_source_table;
            l_error_tab_type (l_error_tab_count).interface_staging_id :=
                                           projects_stg_indx.interface_txn_id;
            l_error_tab_type (l_error_tab_count).ERROR_TYPE := g_err_val;
            l_error_tab_type (l_error_tab_count).ERROR_CODE := l_error_code;
            l_error_tab_type (l_error_tab_count).error_message :=
                                                              l_error_message;
            l_error_tab_type (l_error_tab_count).source_column_name :=
                                                           'LEG_PROJECT_TYPE';
            l_error_tab_type (l_error_tab_count).source_column_value :=
                                           projects_stg_indx.leg_project_type;
         END IF;

         l_validation_flag :=
            validate_project_status (l_errbuf,
                                     l_retcode,
                                     projects_stg_indx.leg_prj_status
                                    );
         DEBUG (   'validate_project_status. l_error_count '
                || l_error_count
                || ' l_retcode '
                || l_retcode
                || ' l_errbuf '
                || l_errbuf
               );

         IF l_retcode <> g_normal
         THEN
            l_error_count := l_error_count + 1;
            l_error_tab_count := l_error_tab_count + 1;
            l_error_message := SUBSTR (l_errbuf, 1, 2000);
            l_error_tab_type (l_error_tab_count).source_table :=
                                                               g_source_table;
            l_error_tab_type (l_error_tab_count).interface_staging_id :=
                                           projects_stg_indx.interface_txn_id;
            l_error_tab_type (l_error_tab_count).ERROR_TYPE := g_err_val;
            l_error_tab_type (l_error_tab_count).ERROR_CODE := l_error_code;
            l_error_tab_type (l_error_tab_count).error_message :=
                                                              l_error_message;
            l_error_tab_type (l_error_tab_count).source_column_name :=
                                                             'LEG_PRJ_STATUS';
            l_error_tab_type (l_error_tab_count).source_column_value :=
                                             projects_stg_indx.leg_prj_status;
         END IF;

         IF l_error_count > 0
         THEN
            l_projects_cur_t (l_loop_count).process_flag := g_flag_error;
            l_projects_cur_t (l_loop_count).ERROR_TYPE := g_err_val;
         ELSE
      BEGIN
        SELECT 'Y'
          INTO l_org_template_null_flag
        FROM xxpa_open_projects_stg
         WHERE interface_txn_id = projects_stg_indx.interface_txn_id
           AND (org_id IS NULL OR carrying_out_organization_id IS NULL OR project_template_id IS NULL);

         l_projects_cur_t (l_loop_count).process_flag := g_flag_error;
               l_projects_cur_t (l_loop_count).ERROR_TYPE := g_err_val;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
          NULL;
         WHEN OTHERS THEN
           l_projects_cur_t (l_loop_count).process_flag := g_flag_error;
                 l_projects_cur_t (l_loop_count).ERROR_TYPE := g_err_val;
      END;


     END IF;

         DEBUG ('001. l_projects_cur_t.COUNT ' || l_projects_cur_t.COUNT);

         /* Logging error for records failed validation (after reaching the LIMIT clause) */
         IF     l_error_tab_type.COUNT > 0
            AND MOD (l_error_tab_type.COUNT, g_limit) = 0
         THEN
            log_error (p_source_tab_type      => l_error_tab_type,
                       pov_return_status      => l_log_ret_sts,
                       pov_error_message      => l_log_err_msg
                      );
            l_error_tab_type.DELETE;
            l_error_tab_count := 0;
         END IF;

         /* Updating Process Flag and Error Type in staging table to V/E as per validation result of each record (after reaching the LIMIT clause) */
         IF l_projects_cur_t.COUNT > 0 AND MOD (l_loop_count, g_limit) = 0
         THEN
            BEGIN
               FORALL indx IN 1 .. l_projects_cur_t.COUNT SAVE EXCEPTIONS
                  UPDATE xxpa_open_projects_stg
                     SET process_flag = l_projects_cur_t (indx).process_flag,
                         ERROR_TYPE = l_projects_cur_t (indx).ERROR_TYPE,
                         request_id = g_request_id,
                         last_updated_date = SYSDATE,
                         last_updated_by = g_user_id,
                         last_update_login = g_login_id
                   WHERE batch_id = g_new_batch_id
                     AND run_sequence_id = g_run_sequence_id
                     AND interface_txn_id =
                                      l_projects_cur_t (indx).interface_txn_id;
            EXCEPTION
               WHEN g_bulk_exception
               THEN
                  DEBUG ('Bulk Exception Count '
                         || SQL%BULK_EXCEPTIONS.COUNT
                        );

                  FOR exep_indx IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
                  LOOP
                     l_blk_err_var1 :=
                        l_projects_cur_t
                               (SQL%BULK_EXCEPTIONS (exep_indx).ERROR_INDEX).interface_txn_id;
                     l_error_message :=
                        SUBSTR
                           (   'Bulk Exception occured while updating project_process_flag. '
                            || SQLERRM
                                  (  -1
                                   * (SQL%BULK_EXCEPTIONS (exep_indx).ERROR_CODE
                                     )
                                  ),
                            1,
                            2000
                           );
                     DEBUG (l_error_message);
                     l_blk_error_tab_type (exep_indx).source_table :=
                                                                g_source_table;
                     l_blk_error_tab_type (exep_indx).interface_staging_id :=
                                                    TO_NUMBER (l_blk_err_var1);
                     l_blk_error_tab_type (exep_indx).source_keyname1 := NULL;
                     l_blk_error_tab_type (exep_indx).source_keyvalue1 := NULL;
                     l_blk_error_tab_type (exep_indx).source_keyname2 := NULL;
                     l_blk_error_tab_type (exep_indx).source_keyvalue2 := NULL;
                     l_blk_error_tab_type (exep_indx).source_keyname3 := NULL;
                     l_blk_error_tab_type (exep_indx).source_keyvalue3 := NULL;
                     l_blk_error_tab_type (exep_indx).source_keyname4 := NULL;
                     l_blk_error_tab_type (exep_indx).source_keyvalue4 := NULL;
                     l_blk_error_tab_type (exep_indx).source_keyname5 := NULL;
                     l_blk_error_tab_type (exep_indx).source_keyvalue5 := NULL;
                     l_blk_error_tab_type (exep_indx).source_column_name :=
                                                            'INTERFACE_TXN_ID';
                     l_blk_error_tab_type (exep_indx).source_column_value :=
                                                                l_blk_err_var1;
                     l_blk_error_tab_type (exep_indx).ERROR_TYPE := g_err_val;
                     l_blk_error_tab_type (exep_indx).ERROR_CODE :=
                                                 'ETN_PA_PROJECT_BLKUPD_EXCEP';
                     l_blk_error_tab_type (exep_indx).error_message :=
                                                               l_error_message;
                     l_blk_error_tab_type (exep_indx).severity := NULL;
                     l_blk_error_tab_type (exep_indx).proposed_solution :=
                                                                          NULL;

                     UPDATE xxpa_open_projects_stg
                        SET process_flag = g_flag_error,
                            ERROR_TYPE = g_err_val,
                            request_id = g_request_id,
                            last_updated_date = SYSDATE,
                            last_updated_by = g_user_id,
                            last_update_login = g_login_id
                      WHERE interface_txn_id = TO_NUMBER (l_blk_err_var1);

                     log_error (p_source_tab_type      => l_blk_error_tab_type,
                                pov_return_status      => l_log_ret_sts,
                                pov_error_message      => l_log_err_msg
                               );
                  END LOOP;
            END;

            l_projects_cur_t.DELETE;
         END IF;
      END LOOP;

      DEBUG ('002. l_projects_cur_t.COUNT ' || l_projects_cur_t.COUNT);

      /* Logging error for records failed validation */
      IF l_error_tab_type.COUNT > 0
      THEN
         log_error (p_source_tab_type      => l_error_tab_type,
                    pov_return_status      => l_log_ret_sts,
                    pov_error_message      => l_log_err_msg
                   );
         l_error_tab_type.DELETE;
      END IF;

      /* Updating Process Flag and Error Type in staging table to V/E as per validation result of each record */
      IF l_projects_cur_t.COUNT > 0
      THEN
         BEGIN
            FORALL indx IN 1 .. l_projects_cur_t.COUNT SAVE EXCEPTIONS
               UPDATE xxpa_open_projects_stg
                  SET process_flag = l_projects_cur_t (indx).process_flag,
                      ERROR_TYPE = l_projects_cur_t (indx).ERROR_TYPE,
                      request_id = g_request_id,
                      last_updated_date = SYSDATE,
                      last_updated_by = g_user_id,
                      last_update_login = g_login_id
                WHERE batch_id = g_new_batch_id
                  AND run_sequence_id = g_run_sequence_id
                  AND interface_txn_id =
                                      l_projects_cur_t (indx).interface_txn_id;
         EXCEPTION
            WHEN g_bulk_exception
            THEN
               DEBUG ('Bulk Exception Count ' || SQL%BULK_EXCEPTIONS.COUNT);

               FOR exep_indx IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  l_blk_err_var1 :=
                     l_projects_cur_t
                               (SQL%BULK_EXCEPTIONS (exep_indx).ERROR_INDEX).interface_txn_id;
                  l_error_message :=
                     SUBSTR
                        (   'Bulk Exception occured while updating project_process_flag. '
                         || SQLERRM
                                (  -1
                                 * (SQL%BULK_EXCEPTIONS (exep_indx).ERROR_CODE
                                   )
                                ),
                         1,
                         2000
                        );
                  DEBUG (l_error_message);
                  l_blk_error_tab_type (exep_indx).source_table :=
                                                                g_source_table;
                  l_blk_error_tab_type (exep_indx).interface_staging_id :=
                                                    TO_NUMBER (l_blk_err_var1);
                  l_blk_error_tab_type (exep_indx).source_keyname1 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyvalue1 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyname2 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyvalue2 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyname3 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyvalue3 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyname4 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyvalue4 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyname5 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyvalue5 := NULL;
                  l_blk_error_tab_type (exep_indx).source_column_name :=
                                                            'INTERFACE_TXN_ID';
                  l_blk_error_tab_type (exep_indx).source_column_value :=
                                                                l_blk_err_var1;
                  l_blk_error_tab_type (exep_indx).ERROR_TYPE := g_err_val;
                  l_blk_error_tab_type (exep_indx).ERROR_CODE :=
                                                 'ETN_PA_PROJECT_BLKUPD_EXCEP';
                  l_blk_error_tab_type (exep_indx).error_message :=
                                                               l_error_message;
                  l_blk_error_tab_type (exep_indx).severity := NULL;
                  l_blk_error_tab_type (exep_indx).proposed_solution := NULL;

                  UPDATE xxpa_open_projects_stg
                     SET process_flag = g_flag_error,
                         ERROR_TYPE = g_err_val,
                         request_id = g_request_id,
                         last_updated_date = SYSDATE,
                         last_updated_by = g_user_id,
                         last_update_login = g_login_id
                   WHERE interface_txn_id = TO_NUMBER (l_blk_err_var1);

                  log_error (p_source_tab_type      => l_blk_error_tab_type,
                             pov_return_status      => l_log_ret_sts,
                             pov_error_message      => l_log_err_msg
                            );
               END LOOP;
         END;

         l_projects_cur_t.DELETE;
      END IF;




   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR ('Exception in Procedure validate_project. ' || SQLERRM,
                    1,
                    2000
                   );
         p_retcode := g_error;
         DEBUG (p_errbuf);
   END validate_project;

-- =============================================================================
-- Procedure: update_stg_flags
-- =============================================================================
--   To update staging table flags for error records
-- =============================================================================
--  Input Parameters :
--    No Input Parameters
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   PROCEDURE update_stg_flags (p_errbuf OUT VARCHAR2, p_retcode OUT NUMBER)
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_error_tab_type       xxetn_common_error_pkg.g_source_tab_type;
      l_blk_error_tab_type   xxetn_common_error_pkg.g_source_tab_type;
      l_log_ret_sts          VARCHAR2 (2000);
      l_log_err_msg          VARCHAR2 (2000);
      l_error_message        VARCHAR2 (2000);
      l_blk_err_var1         VARCHAR2 (2000);
   BEGIN
      p_errbuf := NULL;
      p_retcode := g_normal;

      IF g_run_mode = g_run_mode_validate
      THEN

        BEGIN

           UPDATE xxpa_open_projects_stg xops
              SET xops.process_flag = g_flag_error,
                  xops.ERROR_TYPE = g_err_val,
                  xops.last_updated_date = SYSDATE,
                  xops.last_updated_by = g_user_id,
                  xops.last_update_login = g_login_id,
                  xops.program_application_id = g_prog_appl_id,
                  xops.program_id = g_program_id,
                  xops.request_id = g_request_id
            WHERE xops.batch_id = g_new_batch_id
              AND xops.run_sequence_id = g_run_sequence_id
              AND xops.process_flag = g_flag_ntprocessed
              AND EXISTS (
                     SELECT 1
                       FROM xxetn_common_error xce
                      WHERE xce.source_table = g_source_table
                        AND xce.interface_staging_id = xops.interface_txn_id
                        AND xce.batch_id = xops.batch_id
                        AND xce.run_sequence_id = xops.run_sequence_id);

           DEBUG ('No of records updated ' || SQL%ROWCOUNT);

        EXCEPTION
           WHEN OTHERS
           THEN
              l_error_message :=
                 SUBSTR
                    (   'Exception in updating staging table flags. SQLCODE: '
                     || SQLERRM,
                     1,
                     2000
                    );
              DEBUG (l_error_message);
        END;

      ELSIF g_run_mode = g_run_mode_conversion
      THEN
         /* Updating Process Flag to E/C for error/converted records during conversion run */
         BEGIN
            FORALL indx IN 1 .. g_pass_fail_projects_ttype.COUNT SAVE EXCEPTIONS
               UPDATE xxpa_open_projects_stg xops
                  SET xops.process_flag =
                                g_pass_fail_projects_ttype (indx).process_flag,
                      xops.ERROR_TYPE =
                                  g_pass_fail_projects_ttype (indx).ERROR_TYPE,
                      xops.last_updated_date = SYSDATE,
                      xops.last_updated_by = g_user_id,
                      xops.last_update_login = g_login_id,
                      xops.program_application_id = g_prog_appl_id,
                      xops.program_id = g_program_id,
                      xops.request_id = g_request_id
                WHERE xops.interface_txn_id =
                            g_pass_fail_projects_ttype (indx).interface_txn_id;
         EXCEPTION
            WHEN g_bulk_exception
            THEN
               DEBUG ('Bulk Exception Count ' || SQL%BULK_EXCEPTIONS.COUNT);

               FOR exep_indx IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  l_blk_err_var1 :=
                     g_pass_fail_projects_ttype
                               (SQL%BULK_EXCEPTIONS (exep_indx).ERROR_INDEX).interface_txn_id;
                  l_error_message :=
                     SUBSTR
                        (   'Bulk Exception occured while updating project_process_flag. '
                         || SQLERRM
                                (  -1
                                 * (SQL%BULK_EXCEPTIONS (exep_indx).ERROR_CODE
                                   )
                                ),
                         1,
                         2000
                        );
                  DEBUG (l_error_message);
                  l_blk_error_tab_type (exep_indx).source_table :=
                                                                g_source_table;
                  l_blk_error_tab_type (exep_indx).interface_staging_id :=
                                                    TO_NUMBER (l_blk_err_var1);
                  l_blk_error_tab_type (exep_indx).source_keyname1 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyvalue1 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyname2 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyvalue2 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyname3 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyvalue3 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyname4 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyvalue4 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyname5 := NULL;
                  l_blk_error_tab_type (exep_indx).source_keyvalue5 := NULL;
                  l_blk_error_tab_type (exep_indx).source_column_name :=
                                                            'INTERFACE_TXN_ID';
                  l_blk_error_tab_type (exep_indx).source_column_value :=
                                                                l_blk_err_var1;
                  l_blk_error_tab_type (exep_indx).ERROR_TYPE := g_err_imp;
                  l_blk_error_tab_type (exep_indx).ERROR_CODE :=
                                                 'ETN_PA_PROJECT_BLKUPD_EXCEP';
                  l_blk_error_tab_type (exep_indx).error_message :=
                                                               l_error_message;
                  l_blk_error_tab_type (exep_indx).severity := NULL;
                  l_blk_error_tab_type (exep_indx).proposed_solution := NULL;

                  UPDATE xxpa_open_projects_stg
                     SET process_flag = g_flag_error,
                         ERROR_TYPE = g_err_val
                   WHERE interface_txn_id = TO_NUMBER (l_blk_err_var1);

                  log_error (p_source_tab_type      => l_blk_error_tab_type,
                             pov_return_status      => l_log_ret_sts,
                             pov_error_message      => l_log_err_msg
                            );
               END LOOP;
         END;

         --
         FOR indx IN 1 .. g_pass_fail_projects_ttype.COUNT
         LOOP
            l_error_tab_type (indx).source_table := g_source_table;
            l_error_tab_type (indx).interface_staging_id :=
                           g_pass_fail_projects_ttype (indx).interface_txn_id;
            l_error_tab_type (indx).source_keyname1 := NULL;
            l_error_tab_type (indx).source_keyvalue1 := NULL;
            l_error_tab_type (indx).source_keyname2 := NULL;
            l_error_tab_type (indx).source_keyvalue2 := NULL;
            l_error_tab_type (indx).source_keyname3 := NULL;
            l_error_tab_type (indx).source_keyvalue3 := NULL;
            l_error_tab_type (indx).source_keyname4 := NULL;
            l_error_tab_type (indx).source_keyvalue4 := NULL;
            l_error_tab_type (indx).source_keyname5 := NULL;
            l_error_tab_type (indx).source_keyvalue5 := NULL;
            l_error_tab_type (indx).source_column_name := 'INTERFACE_TXN_ID';
            l_error_tab_type (indx).source_column_value :=
                           g_pass_fail_projects_ttype (indx).interface_txn_id;
            l_error_tab_type (indx).ERROR_TYPE := g_err_imp;
            l_error_tab_type (indx).ERROR_CODE := 'ETN_PA_PROJECT_IMPORT_ERR';
            l_error_tab_type (indx).error_message :=
                                    g_pass_fail_projects_ttype (indx).MESSAGE;
            l_error_tab_type (indx).proposed_solution := NULL;
            l_error_tab_type (indx).severity := NULL;
         END LOOP;

         DEBUG ('l_error_tab_type.COUNT ' || l_error_tab_type.COUNT);

         IF l_error_tab_type.COUNT > 0
         THEN
            log_error (p_source_tab_type      => l_error_tab_type,
                       pov_return_status      => l_log_ret_sts,
                       pov_error_message      => l_log_err_msg
                      );
            l_error_tab_type.DELETE;
         END IF;

         --
         g_pass_fail_projects_ttype.DELETE;
      END IF;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR ('Exception in Procedure update_stg_flags. ' || SQLERRM,
                    1,
                    2000
                   );
         p_retcode := g_warning;
         DEBUG (p_errbuf);
         ROLLBACK;
   END update_stg_flags;

-- =============================================================================
-- Procedure: validate_data
-- =============================================================================
--   Wrapper to control the flow of data validation
-- =============================================================================
--  Input Parameters :
--    No Input Parameters
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   PROCEDURE validate_data (p_errbuf OUT VARCHAR2, p_retcode OUT NUMBER)
   IS
      l_errbuf    VARCHAR2 (2000) := NULL;
      l_retcode   NUMBER          := NULL;
   BEGIN
      /* call to assign_batch_id procedure to assign Batch Id and Run Sequence Id */
      DEBUG ('Call Procedure assign_batch_id - Start');


      assign_batch_id (l_errbuf, l_retcode);
      DEBUG (   'Call Procedure assign_batch_id - End; l_retcode '
             || l_retcode
             || ' l_errbuf '
             || l_errbuf
            );

      IF l_retcode = g_normal
      THEN
         /* call to  derive_organization to derive Project Org and Operating Unit Id */
         DEBUG ('Call Procedure derive_organization - Start');
         derive_organization (l_errbuf, l_retcode);
         DEBUG (   'Call Procedure derive_organization - End; l_retcode '
                || l_retcode
                || ' l_errbuf '
                || l_errbuf
               );
      END IF;

    IF l_retcode = g_normal
      THEN
         /* call to  derive_Operating_Unit procedure to derive Operating Unit Id */
         DEBUG ('Call Procedure derive_Operating_Unit - Start');
         derive_Operating_Unit (l_errbuf, l_retcode);
         DEBUG (   'Call Procedure derive_Operating_Unit - End; l_retcode '
                || l_retcode
                || ' l_errbuf '
                || l_errbuf
               );
      END IF;
      IF l_retcode = g_normal
      THEN
         /* call to derive_project_template procedure to derive_organization to derive Project Template Id */
         DEBUG ('Call Procedure derive_project_template - Start');
         derive_project_template (l_errbuf, l_retcode);
         DEBUG (   'Call Procedure derive_project_template - End; l_retcode '
                || l_retcode
                || ' l_errbuf '
                || l_errbuf
               );
         DEBUG (' ');
         --
         /* Calling update_stg_flags procedure to mark records as Process Flag = E for error records */                --1.1
         DEBUG ('Call Procedure update_stg_flags - Start');
         update_stg_flags (l_errbuf, l_retcode);
         DEBUG (   'Call Procedure update_stg_flags - End; l_retcode '
                || l_retcode
                || ' l_errbuf '
                || l_errbuf
               );
         --
            /* call to validate_project procedure to start data validation for each record */
         DEBUG ('Call Procedure validate_project - Start');
         validate_project (l_errbuf, l_retcode);
         DEBUG ('Call Procedure validate_project - End');
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR ('Exception in Procedure validate_data. ' || SQLERRM,
                    1,
                    2000
                   );
         p_retcode := g_error;
         DEBUG (p_errbuf);
   END validate_data;

-- =============================================================================
-- Procedure: import_data
-- =============================================================================
--   To COnvert Validated records
-- =============================================================================
--  Input Parameters :
--    No Input Parameters
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   PROCEDURE import_data (p_errbuf OUT VARCHAR2, p_retcode OUT NUMBER)
   IS
      l_error_tab_type       xxetn_common_error_pkg.g_source_tab_type;
      l_flag_f               VARCHAR2 (1)                             := 'F';
      project_api_error      EXCEPTION;
      l_errbuf               VARCHAR2 (2000)                          := NULL;
      l_retcode              NUMBER                                   := NULL;
      l_log_ret_sts          VARCHAR2 (2000);
      l_log_err_msg          VARCHAR2 (2000);
      l_error_message        VARCHAR2 (2000);
      l_api_err_msg          VARCHAR2 (2000);
      l_project_count        NUMBER                                   := 0;
      l_task_count           NUMBER                                   := 0;
      /* Variables needed for API standard parameters */
      l_api_version_number   NUMBER                                   := 1.0;
      l_workflow_started     VARCHAR2 (1)                       := g_flag_yes;
      l_msg_count            NUMBER;
      l_msg_index            NUMBER;
      l_msg_index_out        NUMBER;
      l_msg_data             VARCHAR2 (2000);
      l_data                 VARCHAR2 (2000);
      l_return_status        VARCHAR2 (1);
      l_project_in           pa_project_pub.project_in_rec_type;
      l_project_out          pa_project_pub.project_out_rec_type;
      l_class_categories     pa_project_pub.class_category_tbl_type;
      l_tasks_in_rec         pa_project_pub.task_in_rec_type;
      l_tasks_in             pa_project_pub.task_in_tbl_type;
      l_tasks_out            pa_project_pub.task_out_tbl_type;

    l_project_status     pa_projects_all.project_status_code%type;

      /* Cursor to fetch validated data from staging table */
      CURSOR projects_stg_cur
      IS
         SELECT   xops.*,
                  NULL AS MESSAGE
             FROM xxpa_open_projects_stg xops
            WHERE xops.process_flag = g_flag_validated
              AND xops.batch_id = g_new_batch_id
              AND xops.run_sequence_id = g_run_sequence_id
         ORDER BY xops.leg_project_number;
   BEGIN

      FOR projects_indx IN projects_stg_cur
      LOOP
         if upper(projects_indx.leg_prj_status) = 'UNAPPROVED' then
        select project_status_code
        into l_project_status
              from pa_project_statuses
        where project_status_name='ETN Unapproved';
     else
        l_project_status := projects_indx.leg_prj_status;
     end if;


     l_msg_count := NULL;
     mo_global.set_policy_context
         (
             'S',
              projects_indx.org_id
             );

     DEBUG ('Calling API pa_interface_utils_pub.set_global_info ' || g_resp_id || '-'||g_user_id);
         pa_interface_utils_pub.set_global_info
                               (p_api_version_number    => l_api_version_number,
                                p_responsibility_id      => g_resp_id,
                                p_user_id          => g_user_id,
                p_resp_appl_id        => g_resp_appl_id,
                p_advanced_proj_sec_flag  => 'N',
                p_calling_mode        => NULL,
                p_operating_unit_id      => projects_indx.org_id,
                                p_msg_count                 => l_msg_count,
                                p_msg_data                  => l_msg_data,
                                p_return_status             => l_return_status
                               );

         IF l_msg_count IS NULL
     THEN
      l_msg_count := 0;
     END IF;

     DEBUG (   'l_msg_count '
             || l_msg_count
             || ' l_return_status '
             || l_return_status
      );

     IF l_msg_count > 0
     THEN
      DEBUG ('Err Msg ' || SUBSTR (fnd_msg_pub.get (1, l_flag_f), 1, 1000));
     END IF;

     IF l_msg_count = 0
     THEN


           l_project_count := l_project_count + 1;
            g_pass_fail_projects_ttype (l_project_count).interface_txn_id :=
                                               projects_indx.interface_txn_id;
            g_pass_fail_projects_ttype (l_project_count).process_flag :=
                                                             g_flag_completed;
            g_pass_fail_projects_ttype (l_project_count).ERROR_TYPE := NULL;
            g_pass_fail_projects_ttype (l_project_count).MESSAGE := NULL;
            /* Assiging values Project record type */
            l_project_in.pm_project_reference := projects_indx.interface_txn_id;

            l_project_in.pa_project_number :=
                                             projects_indx.leg_project_number;
            l_project_in.project_name := projects_indx.leg_project_name;
            l_project_in.long_name := projects_indx.leg_project_long_name;
            l_project_in.created_from_project_id :=
                                            projects_indx.project_template_id;
            l_project_in.carrying_out_organization_id :=
                                   projects_indx.carrying_out_organization_id;
            l_project_in.public_sector_flag :=
                                              projects_indx.leg_public_sector;
            l_project_in.project_status_code := l_project_status; --projects_indx.leg_prj_status;
            l_project_in.description := projects_indx.leg_prj_description;
            l_project_in.start_date :=
               TO_CHAR (TRUNC (projects_indx.leg_prj_trans_duration_from),
                        'DD-MON-YYYY'
                       );
            l_project_in.completion_date :=
               TO_CHAR (TRUNC (projects_indx.leg_prj_trans_duration_to),
                        'DD-MON-YYYY'
                       );
            /* Assigning values to Project classification table type */
            l_class_categories (1).class_category := g_project_class_category;
            l_class_categories (1).class_code :=
                                                projects_indx.leg_project_type;

            /* Defaulting Task values from the Project Template */
            BEGIN
               FOR indx IN (SELECT   pt.task_number,
                                     pt.task_name,
                                     pt.description,
                                     pt.long_task_name,
                                     pt.service_type_code,
                                     pt.billable_flag,
                                     pt.retirement_cost_flag,
                                     pt.work_type_id,
                                     pt.receive_project_invoice_flag
                                FROM pa_tasks pt
                               WHERE pt.project_id =
                                            projects_indx.project_template_id
                            ORDER BY pt.task_number)
               LOOP
                  l_task_count := l_task_count + 1;
                  l_tasks_in_rec.pa_task_number := indx.task_number;
                  l_tasks_in_rec.task_name := indx.task_name;
                  l_tasks_in_rec.task_description := indx.description;
                  l_tasks_in_rec.long_task_name := indx.long_task_name;
                  l_tasks_in_rec.service_type_code := indx.service_type_code;
                  l_tasks_in_rec.task_start_date :=
                     TO_CHAR
                           (TRUNC (projects_indx.leg_prj_trans_duration_from),
                            'DD-MON-YYYY'
                           );
                  l_tasks_in_rec.task_completion_date := '';
                  l_tasks_in_rec.pm_task_reference :=indx.task_number;
                  l_tasks_in_rec.pm_parent_task_reference := '';
                  l_tasks_in_rec.billable_flag := indx.billable_flag;
                  l_tasks_in_rec.retirement_cost_flag :=
                                                     indx.retirement_cost_flag;
                  l_tasks_in_rec.work_type_id := indx.work_type_id;
                  l_tasks_in_rec.receive_project_invoice_flag :=
                                             indx.receive_project_invoice_flag;
                  l_tasks_in (l_task_count) := l_tasks_in_rec;
               END LOOP;
            END;

            l_task_count := 0;

            BEGIN
               pa_project_pub.init_project;
               DEBUG
                  ('Initialized Project Public API pa_project_pub.init_project'
                  );
               SAVEPOINT create_project;
               /* Calling Project public API will to create Projects and Tasks */
               pa_project_pub.create_project
                                (p_api_version_number      => l_api_version_number
                                                                                  --IN
               ,
                                 p_commit                  => l_flag_f    --IN
                                                                      ,
                                 p_init_msg_list           => l_flag_f    --IN
                                                                      ,
                                 p_msg_count               => l_msg_count
                                                                         --OUT
                                                                         ,
                                 p_msg_data                => l_msg_data --OUT
                                                                        ,
                                 p_return_status           => l_return_status
                                                                             --OUT
               ,
                                 p_workflow_started        => l_workflow_started
                                                                                --OUT
               ,
                             --    p_pm_product_code         => 'MSPROJECT' --IN <AP> Commented and added by AP for Defect# 58
                                 p_pm_product_code         => projects_indx.leg_source_system --'MSPROJECT' --IN
                                                                         ,
                                 p_project_in              => l_project_in
                                                                          --IN
                                                                          ,
                                 p_project_out             => l_project_out
                                                                         --OUT
                                                                           ,
                                 p_class_categories        => l_class_categories
                                                                                --IN
               ,
                                 p_tasks_in                => l_tasks_in  --IN
                                                                        ,
                                 p_tasks_out               => l_tasks_out
                                                                         --OUT
                                );
               DEBUG
                    ('Called Project Public API pa_project_pub.create_project');
               DEBUG (   'l_msg_count '
                      || l_msg_count
                      || ' l_return_status '
                      || l_return_status
                     );
               DEBUG ('New Project Id:     ' || l_project_out.pa_project_id);
               DEBUG ('New Project Number: '
                      || l_project_out.pa_project_number
                     );

               /* Checking for API Success/Faliure */
               IF l_return_status <> g_flag_success
               THEN
                  g_pass_fail_projects_ttype (l_project_count).process_flag :=
                                                                 g_flag_error;
                  g_pass_fail_projects_ttype (l_project_count).ERROR_TYPE :=
                                                                    g_err_imp;
                  RAISE project_api_error;
               END IF;
            EXCEPTION
               /* Exception to handle API erros and mark them as Error for updating into stagin table */
               WHEN project_api_error
               THEN
                  DEBUG ('EXCEPTION WHEN project_api_error ' || l_msg_count);

                  IF l_msg_count >= 1
                  THEN
                     FOR excep_indx IN 1 .. l_msg_count
                     LOOP
                        pa_interface_utils_pub.get_messages
                                          (p_msg_data           => l_msg_data,
                                           p_encoded            => l_flag_f,
                                           p_data               => l_data,
                                           p_msg_count          => l_msg_count,
                                           p_msg_index          => l_msg_index,
                                           p_msg_index_out      => l_msg_index_out
                                          );
                        l_api_err_msg :=
                           SUBSTR (   l_api_err_msg
                                   || ' - '
                                   || SUBSTR (fnd_msg_pub.get (excep_indx,
                                                               l_flag_f
                                                              ),
                                              1,
                                              1000
                                             ),
                                   1,
                                   1000
                                  );
                        DEBUG (   'l_msg_index '
                               || l_msg_index
                               || ' l_msg_index_out '
                               || l_msg_index_out
                               || ' l_data '
                               || l_data
                              );
                        DEBUG ('l_api_err_msg ' || l_api_err_msg);
                        g_pass_fail_projects_ttype (l_project_count).MESSAGE :=
                                                                 l_api_err_msg;
                     END LOOP;
            l_api_err_msg := NULL;
                     ROLLBACK TO create_project;
                  END IF;
               WHEN OTHERS
               THEN
                  /* Exception to any other erros and mark them as Error for updating into stagin table */
                  DEBUG ('EXCEPTION WHEN OTHERS ' || l_msg_count);

                  IF l_msg_count >= 1
                  THEN
                     FOR excep_indx IN 1 .. l_msg_count
                     LOOP
                        pa_interface_utils_pub.get_messages
                                          (p_msg_data           => l_msg_data,
                                           p_encoded            => l_flag_f,
                                           p_data               => l_data,
                                           p_msg_count          => l_msg_count,
                                           p_msg_index          => l_msg_index,
                                           p_msg_index_out      => l_msg_index_out
                                          );
                        l_api_err_msg :=
                           SUBSTR (   l_api_err_msg
                                   || ' - '
                                   || SUBSTR (fnd_msg_pub.get (excep_indx,
                                                               l_flag_f
                                                              ),
                                              1,
                                              1000
                                             ),
                                   1,
                                   1000
                                  );
                        DEBUG (   'l_msg_index '
                               || l_msg_index
                               || ' l_msg_index_out '
                               || l_msg_index_out
                               || ' l_data '
                               || l_data
                              );
                        DEBUG ('l_api_err_msg ' || l_api_err_msg);
                        g_pass_fail_projects_ttype (l_project_count).MESSAGE :=
                                                                 l_api_err_msg;
                     END LOOP;
            l_api_err_msg := NULL;
                     ROLLBACK TO create_project;
                  END IF;
            END;

            --
              /* Calling update_stg_flags procedure to mark records as Process Flag = E/C, in staging table, based on the API Failure/Success */
            IF MOD (l_project_count, g_limit) = 0
            THEN
               DEBUG ('Call Procedure update_stg_flags - Start');
               update_stg_flags (l_errbuf, l_retcode);
               DEBUG (   'Call Procedure update_stg_flags - End; l_retcode '
                      || l_retcode
                      || ' l_errbuf '
                      || l_errbuf
                     );
            END IF;
         --
         END IF;

    END LOOP;                                              --Project Loop

      --
      /* Calling update_stg_flags procedure to mark records as Process Flag = E/C, in staging table, based on the API Failure/Success */
      DEBUG ('Call Procedure update_stg_flags - Start');
      update_stg_flags (l_errbuf, l_retcode);
      DEBUG (   'Call Procedure update_stg_flags - End; l_retcode '
                || l_retcode
                || ' l_errbuf '
                || l_errbuf
               );

   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
            SUBSTR ('Exception in Procedure import_data. ' || SQLERRM,
                    1,
                    2000
                   );
              DEBUG(   'Error : Backtace : '
                            || DBMS_UTILITY.format_error_backtrace
                           );
         p_retcode := g_error;
         DEBUG (p_errbuf);
   END import_data;

-- =============================================================================
-- Procedure: main
-- =============================================================================
--   Main Procedure - Called from Concurrent Program
-- =============================================================================
--  Input Parameters :
--    p_run_mode         : Run Mode ('LOAD-DATA','PRE-VALIDATE','VALIDATE'
--                                  ,'CONVERSION', 'RECONCILE')
--    p_dummy_1          : Dummy Parameter 1
--    p_batch_id         : Batch Id
--    p_dummy_2          : Dummy Parameter 2
--    p_process_records  : Process Records ('ALL', 'ERROR', 'UNPROCESSED')
--  Output Parameters :
--    p_retcode   : Program Return Code = 0/1/2
--    p_errbuf    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   PROCEDURE main (
      p_errbuf            OUT      VARCHAR2,
      p_retcode           OUT      NUMBER,
      p_run_mode          IN       VARCHAR2 DEFAULT NULL,
      p_dummy_1           IN       VARCHAR2,
      p_batch_id          IN       VARCHAR2 DEFAULT NULL,
      p_dummy_2           IN       VARCHAR2,
      p_process_records   IN       VARCHAR2 DEFAULT NULL
   )
   IS
      l_log_ret_sts   VARCHAR2 (2000);
      l_log_err_msg   VARCHAR2 (2000);
      l_errbuf        VARCHAR2 (2000) := NULL;
      l_retcode       NUMBER          := NULL;
      l_init_err      VARCHAR2 (200)  := NULL;
      l_step_flag     VARCHAR2 (1)    := g_flag_success;
   BEGIN
      /* Printing program parameters */
      DEBUG ('p_run_mode         ' || p_run_mode);
      DEBUG ('p_batch_id         ' || p_batch_id);
      DEBUG ('p_process_records  ' || p_process_records);
      /* Printing global profile driven placeholder's value */
      DEBUG ('g_request_id       ' || g_request_id);
      DEBUG ('g_prog_appl_id     ' || g_prog_appl_id);
      DEBUG ('g_program_id       ' || g_program_id);
      DEBUG ('g_user_id          ' || g_user_id);
      DEBUG ('g_login_id         ' || g_login_id);
      DEBUG ('g_org_id           ' || g_org_id);
      DEBUG ('g_resp_id          ' || g_resp_id);
      /* Assigning program parameter values to global placeholders */
      g_run_mode := p_run_mode;
      g_batch_id := p_batch_id;
      g_process_records := p_process_records;
      /* Initialization of Debug Framework */
      xxetn_debug_pkg.initialize_debug
                              (pov_err_msg           => l_init_err,
                               piv_program_name      => 'ETN_PA_OPEN_PROJECTS_CNV'
                              );

      /* Checking for Debug Framework initialization result */
      IF l_init_err IS NULL
      THEN
         IF p_run_mode = g_run_mode_conversion AND p_batch_id IS NULL
         THEN
            p_errbuf :=
               'Parameter BatchId should not be null when RunMode is Conversion';
            p_retcode := g_error;
            l_step_flag := g_flag_error;
            DEBUG (p_errbuf);
         ELSIF p_run_mode = g_run_mode_conversion AND p_batch_id IS NOT NULL
         THEN
            g_new_batch_id := g_batch_id;
            l_step_flag := g_flag_success;
         END IF;
      ELSIF l_init_err IS NOT NULL
      THEN
         p_errbuf :=
            SUBSTR ('Error Framework Initialization failed. ' || l_init_err,
                    1,
                    2000
                   );
         p_retcode := g_error;
         l_step_flag := g_flag_error;
         DEBUG (p_errbuf);
      END IF;

      DEBUG ('l_step_flag ' || l_step_flag);

      /* Calling appropriate procedures as per the program Run mode */
      IF l_step_flag = g_flag_success
      THEN
         /* Run Mode = LOAD-DATA */
         IF p_run_mode = g_run_mode_loadata
         THEN
            DEBUG ('Call Procedure load_data - Start');
            load_data (l_errbuf, l_retcode);
            p_errbuf := l_errbuf;
            p_retcode := l_retcode;
            DEBUG (   'Call Procedure load_data - End; p_retcode '
                   || p_retcode
                   || ' p_errbuf '
                   || p_errbuf
                  );
         /* Run Mode = PRE-VALIDATE */
         ELSIF p_run_mode = g_run_mode_prevalidate
         THEN
            DEBUG ('Call Procedure pre_validate - Start');
            pre_validate (l_errbuf, l_retcode);
            p_errbuf := l_errbuf;
            p_retcode := l_retcode;
            DEBUG (   'Call Procedure pre_validate - End; p_retcode '
                   || p_retcode
                   || ' p_errbuf '
                   || p_errbuf
                  );
         /* Run Mode = VALIDATE */
         ELSIF p_run_mode = g_run_mode_validate
         THEN
            DEBUG ('Call Procedure validate - Start');
            validate_data (l_errbuf, l_retcode);
            DEBUG (   'Call Procedure validate - End; p_retcode '
                   || p_retcode
                   || ' p_errbuf '
                   || p_errbuf
                  );
            /* Calling print_report to print program stats after validation */
            print_report (l_errbuf, l_retcode, g_batch_id);
            p_errbuf := l_errbuf;
            p_retcode := l_retcode;
         /* Run Mode = CONVERSION */
         ELSIF p_run_mode = g_run_mode_conversion
         THEN
            DEBUG ('Call Procedure assign_batch_id - Start');
            assign_batch_id (l_errbuf, l_retcode);
            DEBUG (   'Call Procedure assign_batch_id - End; l_retcode '
                   || l_retcode
                   || ' l_errbuf '
                   || l_errbuf
                  );

            IF l_retcode = g_normal
            THEN
               DEBUG ('Call Procedure import_data - Start');
               import_data (l_errbuf, l_retcode);
               DEBUG (   'Call Procedure import_data - End; l_retcode '
                      || l_retcode
                      || ' l_errbuf '
                      || l_errbuf
                     );
               /* Calling print_report to print program stats after conversion */
               print_report (l_errbuf, l_retcode, g_new_batch_id);
               p_errbuf := l_errbuf;
               p_retcode := l_retcode;
            END IF;
         /* Run Mode = RECONCILE */
         ELSIF p_run_mode = g_run_mode_reconcilition
         THEN
            /* Calling print_report to print program stats */
            print_report (l_errbuf, l_retcode, g_batch_id);
            p_errbuf := l_errbuf;
            p_retcode := l_retcode;
         END IF;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_errbuf :=
             SUBSTR ('Error in Procedure-main. SQLERRM ' || SQLERRM, 1, 2000);
         p_retcode := g_error;
         DEBUG (p_errbuf);
   END main;
END xxpa_prjtsk_cnv_pkg;
/

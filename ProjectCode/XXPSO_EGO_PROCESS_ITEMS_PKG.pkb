CREATE OR REPLACE PACKAGE BODY XXPSO_EGO_PROCESS_ITEMS_PKG
AS
/*=============================================================================================+
|                               Copyright (c) 2016 Pearson                                     |
|                                 All rights reserved.                                         |
+==============================================================================================+
|
| Header            : $1.0
| File              : XXPSO_EGO_PROCESS_ITEMS_PKG.pkb
| Package Name      : XXPSO_EGO_PROCESS_ITEMS_PKG
| Developed By      : Narendra Mishra
| Description       : Package Body for Item Interface
|
|  ***************************************** IMPORTANT! *****************************************
|   If you need to change this file please ensure you :
|           a) Increment the version number and update the change history.
|           b) Comment the code with the version number and initials.
|           c) Update the technical design document.
|  **********************************************************************************************
|
|  Change History :
|
|  Who                    When            Version    Change (include Bug#if appropriate)
|+----------------------+---------------+----------+--------------------------------------------+
|  Narendra Mishra      | 28-JAN-2016   | 1.0      | Initial Revision
|  Narendra Mishra      | 17-Mar-2016   | 1.1      | Added Batch Size Parameter in Main Procedure, Added update_batch_id, gather_table_stats procedures
|  Narendra Mishra      | 21-Mar-2016   | 1.2      | Added create_import_batch Function
|  Narendra Mishra      | 22-Mar-2016   | 1.3      | Added purge_if_tables and archive_stg_tables Procedure
|  Narendra Mishra      | 23-Mar-2016   | 1.4      | Added create_vset_value Function
|  Narendra Mishra      | 25-Apr-2016   | 1.5      | Added perform_de_duplication Procedure
|+----------------------+---------------+----------+--------------------------------------------+
|
+==============================================================================================*/


/* ********************************************************
   * Procedure: Log_Msg
   *
   * Synopsis: This procedure is to print log messages
   *
   * Parameters:
   *   OUT:
   *   IN:
   *        p_msg        VARCHAR2        -- Log Messages
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            28-JAN-2016
   ************************************************************************************* */
    PROCEDURE Log_Msg (p_msg IN VARCHAR2)
    IS
        lc_msg  VARCHAR2 (4000) := p_msg;
    BEGIN
         fnd_file.put_line (fnd_file.LOG, lc_msg);
         dbms_output.put_line(p_msg);
    EXCEPTION WHEN OTHERS
    THEN
        lc_msg := 'Unhandled exception in Log_Msg. Error: '||SQLCODE||'->'||SQLERRM;
        fnd_file.put_line (fnd_file.LOG, lc_msg);
    END Log_Msg;



/* ********************************************************
   * Procedure: Out_Msg
   *
   * Synopsis: This procedure is to print log messages
   *
   * Parameters:
   *   OUT:
   *   IN:
   *        p_msg        VARCHAR2        -- Log Messages
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            28-JAN-2016
   ************************************************************************************* */
    PROCEDURE Out_Msg (p_msg IN VARCHAR2)
    IS
        lc_msg  VARCHAR2 (4000) := p_msg;
    BEGIN
         fnd_file.put_line (fnd_file.OUTPUT, lc_msg);
    EXCEPTION WHEN OTHERS
    THEN
        lc_msg := 'Unhandled exception in Out_Msg. Error: '||SQLCODE||'->'||SQLERRM;
        fnd_file.put_line (fnd_file.LOG, lc_msg);
    END Out_Msg;


/* ********************************************************
   * Procedure: Debug_Msg
   *
   * Synopsis: This procedure is to print debug messages
   *
   * Parameters:
   *   OUT:
   *   IN:
   *        p_msg        VARCHAR2        -- Log Messages
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            28-JAN-2016
   ************************************************************************************* */
    PROCEDURE Debug_Msg (p_msg IN VARCHAR2)
    IS
        lc_msg  VARCHAR2 (4000) := p_msg;
    BEGIN
        IF gc_debug_flag = 'Y'
        THEN
            fnd_file.put_line (fnd_file.LOG, lc_msg);
        END IF;
    EXCEPTION WHEN OTHERS
    THEN
        lc_msg := 'Unhandled exception in Debug_Msg. Error: '||SQLCODE||'->'||SQLERRM;
        Log_Msg( lc_msg);
    END Debug_Msg;

    

/* ********************************************************
   * Procedure: capture_error
   *
   * Synopsis: This procedure is to Log an error in Common Error Table.
   *
   * Parameters:
   *   OUT:
   *   IN:
   *        p_pri_identifier        VARCHAR2        -- Primary Record Identifier
   *        p_sec_identifier        VARCHAR2        -- Secondary Record Identifier
   *        p_ter_identifier        VARCHAR2        -- Third Record Identifier
   *        p_error_code            VARCHAR2        -- Error Code
   *        p_error_column          VARCHAR2        -- Column having Error
   *        p_error_value           VARCHAR2        -- Value in Error Column
   *        p_error_desc            VARCHAR2        -- Error Desc
   *        p_req_action            VARCHAR2        -- required Action to resolve error
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            28-JAN-2016
   ************************************************************************************* */
    PROCEDURE capture_error
    (
     p_ricew_id            IN VARCHAR2
    ,p_pri_identifier      IN VARCHAR2
    ,p_sec_identifier      IN VARCHAR2
    ,p_ter_identifier      IN VARCHAR2
    ,p_error_code          IN VARCHAR2
    ,p_error_column        IN VARCHAR2
    ,p_error_value         IN VARCHAR2
    ,p_error_desc          IN VARCHAR2
    ,p_req_action          IN VARCHAR2
    ,p_data_source         IN VARCHAR2
    )
    IS
        lc_msg      VARCHAR2(4000) := NULL;
    BEGIN

        XXPSO_CMN_CNV_PKG.log_error_msg
                                ( p_ricew_id       => p_ricew_id
                                 ,p_track          => 'CMN'
                                 ,p_source         => 'CONVERSION'
                                 ,p_calling_object => 'XXPSO_EGO_PROCESS_ITEMS_PKG'
                                 ,p_pri_record_id  => p_pri_identifier
                                 ,p_sec_record_id  => p_sec_identifier
                                 ,p_ter_record_id  => p_ter_identifier
                                 ,p_err_code       => p_error_code
                                 ,p_err_column     => p_error_column
                                 ,p_err_value      => p_error_value
                                 ,p_err_desc       => p_error_desc
                                 ,p_rect_action    => p_req_action
                                 ,p_debug_flag     => 'N'
                                 ,p_request_id     => fnd_global.CONC_REQUEST_ID
                                 );
    EXCEPTION WHEN OTHERS THEN
        lc_msg := 'Unhandled Exception in capture_error procedure. Error Code: '||SQLCODE||' -> '||SQLERRM;
        Log_Msg( lc_msg);
    END capture_error;



/* ********************************************************
   * Function: create_import_batch
   *
   * Synopsis: This function is to create EGO Import Batches for the Given Source System
   *
   * Parameters:
   *   OUT:
   *   IN:
   *        p_source_system_id        NUMBER        --
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            21-Mar-2016
   ************************************************************************************* */
    FUNCTION create_import_batch (p_source_system_id NUMBER)
        RETURN NUMBER
    IS
        lc_msg                  VARCHAR2(4000) := NULL;
        l_batch_id              NUMBER;
        l_option_set_id         NUMBER;
        l_import_batches_rowid  VARCHAR2(1000);
        l_batche_name           VARCHAR2(1000);

        CURSOR c_batch
        IS
           SELECT hosv.orig_system, eibt.name, eibb.*
             FROM ego_import_batches_b eibb, ego_import_batches_tl eibt, hz_orig_systems_vl hosv
            WHERE eibb.source_system_id   = hosv.orig_system_id
              AND eibb.batch_id           = eibt.batch_id
              AND eibb.batch_status       = 'A'
              AND hosv.end_date_active    IS NULL
              AND hosv.status             = 'A'
              --AND hosv.orig_system        = 'EPM'
              AND hosv.orig_system_id     = p_source_system_id
              AND rownum                  = 1;

        CURSOR c_imp_set (p_batch_id NUMBER)
        IS
           SELECT *
             FROM EGO_IMPORT_OPTION_SETS
            WHERE batch_id      = p_batch_id
              AND rownum        = 1;

    BEGIN

        FOR c1 IN c_batch
        LOOP

            BEGIN
                SELECT MTL_SYSTEM_ITEMS_INTF_SETS_S.nextval INTO l_batch_id FROM dual;
                SELECT EGO_IMPORT_OPTION_SETS_S.nextval INTO l_option_set_id FROM dual;
                l_batche_name   := c1.orig_system || '-' || TO_CHAR(SYSDATE,'YYYYMMDD') || '-' || TO_CHAR(l_batch_id);
            EXCEPTION WHEN OTHERS
            THEN
                l_batch_id          := NULL;
                l_option_set_id     := NULL;
                l_batche_name       := NULL;
            END;

            Debug_Msg('l_batche_name     : '|| l_batche_name);
            Debug_Msg('l_batch_id        : '|| l_batch_id);
            Debug_Msg('l_option_set_id   : '|| l_option_set_id);

            IF l_batch_id IS NULL
            THEN
                RETURN (-1);
            ELSE

                EGO_IMPORT_BATCHES_PKG.INSERT_ROW
                          (X_ROWID                 => l_import_batches_rowid,
                           X_BATCH_ID              => l_batch_id,
                           X_ORGANIZATION_ID       => c1.organization_id,
                           X_SOURCE_SYSTEM_ID      => c1.source_system_id,
                           X_BATCH_TYPE            => c1.batch_type,
                           X_ASSIGNEE              => c1.assignee,
                           X_BATCH_STATUS          => c1.batch_status,
                           X_OBJECT_VERSION_NUMBER => c1.object_version_number,
                           X_NAME                  => l_batche_name,
                           X_DESCRIPTION           => l_batche_name,
                           X_CREATION_DATE         => sysdate,
                           X_CREATED_BY            => fnd_global.user_id,
                           X_LAST_UPDATE_DATE      => sysdate,
                           X_LAST_UPDATED_BY       => fnd_global.user_id,
                           X_LAST_UPDATE_LOGIN     => fnd_global.login_id
                           );

                Debug_Msg('BATCH CREATED ');

                FOR c2 IN c_imp_set(c1.batch_id)
                LOOP

                    INSERT INTO EGO_IMPORT_OPTION_SETS
                          (OPTION_SET_ID,
                           SOURCE_SYSTEM_ID,
                           BATCH_ID,
                           MATCH_ON_DATA_LOAD,
                           DEF_MATCH_RULE_CUST_APP_ID,
                           DEF_MATCH_RULE_CUST_CODE,
                           DEF_MATCH_RULE_RN_APP_ID,
                           DEF_MATCH_RULE_RN_CODE,
                           APPLY_DEF_MATCH_RULE_ALL,
                           CONFIRM_SINGLE_MATCH,
                           ENABLED_FOR_DATA_POOL,
                           IMPORT_ON_DATA_LOAD,
                           REVISION_IMPORT_POLICY,
                           IMPORT_XREF_ONLY,
                           STRUCTURE_TYPE_ID,
                           STRUCTURE_NAME,
                           STRUCTURE_EFFECTIVITY_TYPE,
                           EFFECTIVITY_DATE,
                           FROM_END_ITEM_UNIT_NUMBER,
                           STRUCTURE_CONTENT,
                           CHANGE_ORDER_CREATION,
                           ADD_ALL_TO_CHANGE_FLAG,
                           CHANGE_MGMT_TYPE_CODE,
                           CHANGE_TYPE_ID,
                           CHANGE_NOTICE,
                           CHANGE_NAME,
                           CHANGE_DESCRIPTION,
                           NIR_OPTION,
                           OBJECT_VERSION_NUMBER,
                           CREATED_BY,
                           CREATION_DATE,
                           LAST_UPDATED_BY,
                           LAST_UPDATE_DATE,
                           LAST_UPDATE_LOGIN)
                        VALUES
                          (l_option_set_id,
                           C2.source_system_id,
                           l_batch_id,
                           C2.match_on_data_load,
                           C2.def_match_rule_cust_app_id,
                           C2.def_match_rule_cust_code,
                           C2.def_match_rule_rn_app_id,
                           C2.def_match_rule_rn_code,
                           C2.apply_def_match_rule_all,
                           C2.confirm_single_match,
                           C2.enabled_for_data_pool,
                           C2.import_on_data_load,
                           C2.revision_import_policy,
                           C2.import_xref_only,
                           C2.structure_type_id,
                           C2.structure_name,
                           C2.structure_effectivity_type,
                           C2.effectivity_date,
                           C2.from_end_item_unit_number,
                           C2.structure_content,
                           C2.change_order_creation,
                           C2.add_all_to_change_flag,
                           C2.change_mgmt_type_code,
                           C2.change_type_id,
                           C2.change_notice,
                           C2.change_name,
                           C2.change_description,
                           C2.nir_option,
                           C2.object_version_number,
                           fnd_global.user_id,
                           sysdate,
                           fnd_global.user_id,
                           sysdate,
                           fnd_global.login_id);

                           Debug_Msg('OPTION SET CREATED ');

                END LOOP;

            END IF;

        END LOOP;

        COMMIT;

        RETURN (l_batch_id);

    EXCEPTION WHEN OTHERS
    THEN
        lc_msg := 'Unhandled exception in create_import_batch. Error: '||SQLCODE||'->'||SQLERRM;
        Log_Msg( lc_msg);

        gn_retcode  := gcn_retcode_warning;

        RETURN (-1);

    END create_import_batch;



/* ********************************************************
   * Procedure: gather_table_stats
   *
   * Synopsis: This procedure is to run Gather Table Stats for INV, EGO and XXPSO tables
   *
   * Parameters:
   *   OUT:
   *   IN:
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            17-MAR-2016
   ************************************************************************************* */
    PROCEDURE gather_table_stats
    IS
        lc_msg              VARCHAR2(4000) := NULL;
        l_schema            VARCHAR2 (30);
        l_schema_status     VARCHAR2 (1);
        l_industry          VARCHAR2 (1);
    BEGIN

        BEGIN
            DBMS_STATS.unlock_table_stats ('EGO', 'EGO_ITM_USR_ATTR_INTRFC');
            DBMS_STATS.unlock_table_stats ('INV', 'MTL_SYSTEM_ITEMS_INTERFACE');
            DBMS_STATS.unlock_table_stats ('INV', 'MTL_ITEM_REVISIONS_INTERFACE');
        EXCEPTION
        WHEN OTHERS THEN
            NULL;
        END;

        fnd_stats.load_histogram_cols ('DELETE',
                                       401,
                                       'MTL_SYSTEM_ITEMS_INTERFACE',
                                       'PROCESS_FLAG'
                                      );
        fnd_stats.load_histogram_cols ('DELETE',
                                       401,
                                       'MTL_SYSTEM_ITEMS_INTERFACE',
                                       'SET_PROCESS_ID'
                                      );

        -- Gather Stats fpr EGO Table
        IF (fnd_installation.get_app_info ('EGO',
                                           l_schema_status,
                                           l_industry,
                                           l_schema
                                          )
           )
        THEN
           fnd_stats.gather_table_stats (ownname      => l_schema,
                                         tabname      => 'EGO_ITM_USR_ATTR_INTRFC',
                                         CASCADE      => TRUE
                                        );
        END IF;


        -- Gather Stats fpr INV Table
        IF (fnd_installation.get_app_info ('INV',
                                           l_schema_status,
                                           l_industry,
                                           l_schema
                                          )
           )
        THEN
           fnd_stats.gather_table_stats (ownname      => l_schema,
                                         tabname      => 'MTL_SYSTEM_ITEMS_INTERFACE',
                                         CASCADE      => TRUE
                                        );
           fnd_stats.gather_table_stats (ownname      => l_schema,
                                         tabname      => 'MTL_ITEM_REVISIONS_INTERFACE',
                                         CASCADE      => TRUE
                                        );
           fnd_stats.gather_table_stats (ownname      => l_schema,
                                         tabname      => 'MTL_SYSTEM_ITEMS_B',
                                         CASCADE      => TRUE
                                        );
        END IF;

        -- Gather Stats fpr XXPSO Table
        IF (fnd_installation.get_app_info ('XXPSO',
                                           l_schema_status,
                                           l_industry,
                                           l_schema
                                          )
           )
        THEN
           fnd_stats.gather_table_stats (ownname      => l_schema,
                                         tabname      => 'XXPSO_EGO_ITEMS_STG',
                                         CASCADE      => TRUE
                                        );
           fnd_stats.gather_table_stats (ownname      => l_schema,
                                         tabname      => 'XXPSO_EGO_ITEM_ATTR_STG',
                                         CASCADE      => TRUE
                                        );
        END IF;

        DBMS_STATS.set_column_stats ('EGO',
                                     'EGO_ITM_USR_ATTR_INTRFC',
                                     'PROCESS_STATUS',
                                     distcnt      => 4,
                                     density      => 0.25
                                    );

        DBMS_STATS.lock_table_stats ('EGO', 'EGO_ITM_USR_ATTR_INTRFC');
        DBMS_STATS.lock_table_stats ('INV', 'MTL_SYSTEM_ITEMS_INTERFACE');
        DBMS_STATS.lock_table_stats ('INV', 'MTL_ITEM_REVISIONS_INTERFACE');

        COMMIT;
    EXCEPTION WHEN OTHERS
    THEN
        lc_msg := 'Unhandled exception in gather_table_stats. Error: '||SQLCODE||'->'||SQLERRM;
        Log_Msg( lc_msg);
    END gather_table_stats;
    
    
    

/* ********************************************************
   * Function: create_vset_value
   *
   * Synopsis: This function is to dynamically create value for the given independent value set
   *
   * Parameters:
   *   OUT:
   *   IN:
   *        p_flex_value_set_name        VARCHAR2        --
   *        p_flex_value                 VARCHAR2        --
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            23-Mar-2016
   ************************************************************************************* */
    FUNCTION create_vset_value (p_flex_value_set_name  IN VARCHAR2, p_flex_value  IN VARCHAR2)
        RETURN BOOLEAN
    IS
        lc_msg                  VARCHAR2(4000) := NULL;
        l_storage_value         VARCHAR2(1000);
    BEGIN

        FND_FLEX_VAL_API.create_independent_vset_value
          ( p_flex_value_set_name        => p_flex_value_set_name
           ,p_flex_value                 => p_flex_value
           ,p_description                => p_flex_value
           ,p_enabled_flag               => 'Y'
           ,p_start_date_active          => SYSDATE
           ,p_end_date_active            => NULL
           ,p_summary_flag               => 'N'
           ,p_structured_hierarchy_level => NULL
           ,p_hierarchy_level            => NULL
           ,x_storage_value              => l_storage_value
           );

        IF l_storage_value IS NOT NULL
        THEN
            RETURN TRUE;
        ELSE
            RETURN FALSE;
        END IF;

    EXCEPTION WHEN OTHERS
    THEN
        lc_msg := 'Unhandled exception in create_vset_value. Error: '||SQLCODE||'->'||SQLERRM;
        Log_Msg( lc_msg);
        RETURN FALSE;
    END create_vset_value;    
    
    
    
    
    

/* ********************************************************
   * Procedure: perform_de_duplication
   *
   * Synopsis: This procedure is to find duplicate items based on the source system - master source system defined in lookup XXPSO_SURVIVORSHP
   *
   * Parameters:
   *   OUT:
   *   IN:
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            26-Apr-2016
   ************************************************************************************* */
    PROCEDURE perform_de_duplication
    IS
        lc_msg          VARCHAR2(4000)  := NULL;
        ln_row_cnt      NUMBER          := 0;
    BEGIN
    
        Log_Msg('****************************************************************');
        Log_Msg('***         PERFORMING ITEM DEDUPLICATION - STARTED          ***');    

        -- Finding out duplicate item withing the staging table
         UPDATE xxpso_ego_items_stg ss1
            SET ss1.similar_item_source_system = (   SELECT MAX(ss2.source_system)
                                                       FROM xxpso_ego_items_stg ss2,
                                                            fnd_lookups         lkp
                                                      WHERE lkp.lookup_type         = 'XXPSO_SURVIVORSHP'
                                                        AND lkp.enabled_flag        = 'Y' 
                                                        AND TRUNC(SYSDATE)          BETWEEN TRUNC(lkp.start_date_active) 
                                                                                    AND TRUNC(NVL(lkp.end_date_active,SYSDATE+1))                                                      
                                                        AND lkp.meaning            <> lkp.description
                                                        AND ss1.source_system       = lkp.meaning        -- Secondary SS
                                                        AND ss2.source_system       = lkp.description    -- Master SS
                                                        AND ss2.source_item_number  = ss1.source_item_number
                                                        AND ss2.status_code         = gc_new_flag
                                                 ),
                ss1.similar_item_number = (  SELECT MAX(ss2.source_item_number)
                                               FROM xxpso_ego_items_stg ss2,
                                                    fnd_lookups         lkp
                                              WHERE lkp.lookup_type         = 'XXPSO_SURVIVORSHP'
                                                AND lkp.enabled_flag        = 'Y' 
                                                AND TRUNC(SYSDATE)          BETWEEN TRUNC(lkp.start_date_active) 
                                                                            AND TRUNC(NVL(lkp.end_date_active,SYSDATE+1))    
                                                AND lkp.meaning            <> lkp.description
                                                AND ss1.source_system       = lkp.meaning                -- Secondary SS
                                                AND ss2.source_system       = lkp.description            -- Master SS
                                                AND ss2.source_item_number  = ss1.source_item_number
                                                AND ss2.status_code         = gc_new_flag
                                         )
          WHERE EXISTS (  SELECT 1
                            FROM xxpso_ego_items_stg ss2,
                                fnd_lookups          lkp
                          WHERE lkp.lookup_type         = 'XXPSO_SURVIVORSHP'
                            AND lkp.enabled_flag        = 'Y' 
                            AND TRUNC(SYSDATE)          BETWEEN TRUNC(lkp.start_date_active) 
                                                        AND TRUNC(NVL(lkp.end_date_active,SYSDATE+1))    
                            AND lkp.meaning            <> lkp.description
                            AND ss1.source_system       = lkp.meaning        -- Secondary SS
                            AND ss2.source_system       = lkp.description    -- Master SS
                            AND ss2.source_item_number  = ss1.source_item_number
                            AND ss2.status_code         = gc_new_flag
                        )
           AND ss1.status_code          = gc_new_flag
           AND ss1.similar_item_number  IS NULL;
           
           ln_row_cnt  := NVL(SQL%ROWCOUNT,0);
           Log_Msg('No of Items for which Duplicate Found within Staging Table      : ' || ln_row_cnt);    

           COMMIT;
           
           
        -- Finding out duplicate item from base table
         UPDATE xxpso_ego_items_stg ss1
            SET ss1.similar_item_source_system = (   SELECT MAX(hosv.orig_system)
                                                       FROM mtl_cross_references    mcr,
                                                            hz_orig_systems_vl      hosv,
                                                            fnd_lookups             lkp
                                                      WHERE lkp.lookup_type             = 'XXPSO_SURVIVORSHP'
                                                        AND lkp.enabled_flag            = 'Y' 
                                                        AND TRUNC(SYSDATE)              BETWEEN TRUNC(lkp.start_date_active) 
                                                                                        AND TRUNC(NVL(lkp.end_date_active,SYSDATE+1))                                                      
                                                        AND lkp.meaning                <> lkp.description                                                        
                                                        AND mcr.source_system_id        = hosv.orig_system_id
                                                        AND mcr.cross_reference_type    = 'SS_ITEM_XREF'
                                                        AND mcr.end_date_active         IS NULL
                                                        AND hosv.end_date_active        IS NULL
                                                        AND hosv.status                 = 'A'
                                                        AND ss1.source_system           = lkp.meaning           -- Secondary SS                                                        
                                                        AND hosv.orig_system            = lkp.description       -- Master SS
                                                        AND mcr.cross_reference         = ss1.source_item_number
                                                 ),
                ss1.similar_item_number = (  SELECT MAX(mcr.cross_reference)
                                               FROM mtl_cross_references    mcr,
                                                    hz_orig_systems_vl      hosv,
                                                    fnd_lookups             lkp
                                              WHERE lkp.lookup_type             = 'XXPSO_SURVIVORSHP'
                                                AND lkp.enabled_flag            = 'Y' 
                                                AND TRUNC(SYSDATE)              BETWEEN TRUNC(lkp.start_date_active) 
                                                                                AND TRUNC(NVL(lkp.end_date_active,SYSDATE+1))                                                      
                                                AND lkp.meaning                <> lkp.description                                                        
                                                AND mcr.source_system_id        = hosv.orig_system_id
                                                AND mcr.cross_reference_type    = 'SS_ITEM_XREF'
                                                AND mcr.end_date_active         IS NULL
                                                AND hosv.end_date_active        IS NULL
                                                AND hosv.status                 = 'A'
                                                AND ss1.source_system           = lkp.meaning           -- Secondary SS                                                        
                                                AND hosv.orig_system            = lkp.description       -- Master SS
                                                AND mcr.cross_reference         = ss1.source_item_number
                                         )
          WHERE EXISTS ( SELECT 1
                           FROM mtl_cross_references    mcr,
                                hz_orig_systems_vl      hosv,
                                fnd_lookups             lkp
                          WHERE lkp.lookup_type             = 'XXPSO_SURVIVORSHP'
                            AND lkp.enabled_flag            = 'Y' 
                            AND TRUNC(SYSDATE)              BETWEEN TRUNC(lkp.start_date_active) 
                                                            AND TRUNC(NVL(lkp.end_date_active,SYSDATE+1))                                                      
                            AND lkp.meaning                <> lkp.description                                                        
                            AND mcr.source_system_id        = hosv.orig_system_id
                            AND mcr.cross_reference_type    = 'SS_ITEM_XREF'
                            AND mcr.end_date_active         IS NULL
                            AND hosv.end_date_active        IS NULL
                            AND hosv.status                 = 'A'
                            AND ss1.source_system           = lkp.meaning           -- Secondary SS                                                        
                            AND hosv.orig_system            = lkp.description       -- Master SS
                            AND mcr.cross_reference         = ss1.source_item_number
                        )
           AND ss1.status_code          = gc_new_flag
           AND ss1.similar_item_number  IS NULL;
           
           ln_row_cnt  := NVL(SQL%ROWCOUNT,0);                               
           Log_Msg('No of Items for which Duplicate Found in Oracle Base Table      : ' || ln_row_cnt);             

           COMMIT;
           
        Log_Msg('***         PERFORMING ITEM DEDUPLICATION - COMPLETED        ***');                  
        Log_Msg('****************************************************************');


    EXCEPTION WHEN OTHERS
    THEN
        lc_msg := 'Unhandled exception in perform_de_duplication. Error: '||SQLCODE||'->'||SQLERRM;
        Log_Msg( lc_msg);

        gn_retcode  := gcn_retcode_warning;
    END perform_de_duplication;

    
    

/* ********************************************************
   * Procedure: identify_archive_records
   *
   * Synopsis: This procedure is to identifying archive records
   *
   * Parameters:
   *   OUT:
   *   IN:
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            22-Mar-2016
   ************************************************************************************* */    
    PROCEDURE identify_archive_records
    IS
        lc_msg              VARCHAR2(4000) := NULL;
    BEGIN

        Log_Msg('****************************************************************');
        Log_Msg('****        IDENTIFYING ARCHIVE RECORDS - STARTED          *****');

         UPDATE xxpso_ego_items_stg ss1
            SET ss1.ready_to_archive = gc_yes
          WHERE ss1.status_code         IN (gc_import_succ_flag);
         COMMIT; 
        
         UPDATE xxpso_ego_items_stg ss1
            SET ss1.ready_to_archive = gc_yes
          WHERE EXISTS (  SELECT 1
                            FROM xxpso_ego_items_stg ss2
                          WHERE ss2.source_system       = ss1.source_system    -- Master SS
                            AND ss2.source_item_number  = ss1.source_item_number
                            AND ss2.status_code         = gc_new_flag
                        )
           AND ss1.status_code          IN (gc_valid_error_flag, gc_import_error_flag);
         COMMIT;            

        Log_Msg('****        IDENTIFYING ARCHIVE RECORDS - COMPLETED        *****');
        Log_Msg('****************************************************************');

    EXCEPTION WHEN OTHERS
    THEN
        lc_msg := 'Unhandled exception in identify_archive_records. Error: '||SQLCODE||'->'||SQLERRM;
        Log_Msg( lc_msg);
    END identify_archive_records;    
    
                    
                    
   /* ************************************************************************************
   * Procedure: archive_stg_table
   *
   * Synopsis: This procedure will be used to archive staging tables
   *
   * PARAMETERS:
   *   OUT: 
   *        p_out_ret_status        VARCHAR2        -- 
   *   IN:
   *        p_in_stg_tab_name       VARCHAR2        -- 
   *        p_in_arc_tab_name       VARCHAR2        -- 

   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            29-Apr-2014
   ************************************************************************************* */   
    PROCEDURE archive_stg_table (  p_in_stg_tab_name   IN VARCHAR2
                                  ,p_in_arc_tab_name   IN VARCHAR2
                                  ,p_out_ret_status    OUT VARCHAR2
                                )
    IS
        lc_msg              VARCHAR2(4000)  := NULL;
        ln_row_cnt          NUMBER          := 0;
        l_dyn_sql           VARCHAR2(4000);
        l_updt_stmt	    VARCHAR2(4000);
        
    CURSOR C_REC
    IS 
        SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = p_in_stg_tab_name
        INTERSECT 
        SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = p_in_arc_tab_name
        ORDER BY 1;
        
    BEGIN
    
        -- Preparing Dynamic SQL for Inserts
        l_dyn_sql  :=  'INSERT INTO ' || p_in_arc_tab_name || '( ';

        FOR C IN C_REC
        LOOP
            l_dyn_sql  :=  l_dyn_sql || c.COLUMN_NAME || ', ';            
        END LOOP;

        l_dyn_sql  := SUBSTR(l_dyn_sql, 1, length(l_dyn_sql)-2 );
        l_dyn_sql  :=  l_dyn_sql || ' ) SELECT ' ;

        FOR C IN C_REC
        LOOP
            l_dyn_sql  :=  l_dyn_sql || c.COLUMN_NAME || ', ';            
        END LOOP;

        l_dyn_sql  := SUBSTR(l_dyn_sql, 1, length(l_dyn_sql)-2 );
        l_dyn_sql  :=  l_dyn_sql || ' FROM ' || p_in_stg_tab_name || ' WHERE READY_TO_ARCHIVE = ''Y'' ' ;         

        -- Performing Inserts
        EXECUTE IMMEDIATE l_dyn_sql;
        
        -- Updating the archive_date.
        l_updt_stmt := 'UPDATE '||p_in_arc_tab_name||' SET archive_date = SYSDATE WHERE archive_date IS NULL';
        EXECUTE IMMEDIATE l_updt_stmt;
        
        ln_row_cnt  := NVL(SQL%ROWCOUNT,0);
        Log_Msg('No of records inserted into '|| p_in_arc_tab_name || ' table    : '|| ln_row_cnt);
        
        
        
        COMMIT;

        
        -- Preparing Dynamic SQL for Delete
        l_dyn_sql  := 'DELETE FROM ' || p_in_stg_tab_name || ' WHERE READY_TO_ARCHIVE = ''Y'' ' ;


        EXECUTE IMMEDIATE l_dyn_sql;
        
        ln_row_cnt  := NVL(SQL%ROWCOUNT,0);
        Log_Msg('No of records deleted from '|| p_in_stg_tab_name || ' table    : '|| ln_row_cnt);        
        
        COMMIT;        

        p_out_ret_status := 'SUCESS';
              
        
    EXCEPTION WHEN OTHERS
    THEN
        lc_msg := 'Unhandled exception in archive_stg_table. Error: '||SQLCODE||'->'||SQLERRM;
        Log_Msg( lc_msg);
        p_out_ret_status    := lc_msg;
    END archive_stg_table;   




/* ********************************************************
   * Procedure: archive_stg_tables
   *
   * Synopsis: This procedure is to archive staging tables based on the parameter
   *
   * Parameters:
   *   OUT:
   *   IN:
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            22-Mar-2016
   ************************************************************************************* */
    PROCEDURE archive_stg_tables
    IS
        lc_msg              VARCHAR2(4000) := NULL;
    BEGIN

        Log_Msg('****************************************************************');
        Log_Msg('****          ARCHIVE STAGING TABLES - STARTED             *****');
        
        
        archive_stg_table('XXPSO_EGO_ITEMS_STG','XXPSO_EGO_ITEMS_STG_ARCH',lc_msg);
        
        IF lc_msg = 'SUCESS'
        THEN
            Log_Msg( 'Archive for Item Staging Completed Sucessfully');
        END IF;
        
        archive_stg_table('XXPSO_EGO_ITEM_ATTR_STG','XXPSO_EGO_ITEM_ATTR_STG_ARCH',lc_msg);

        IF lc_msg = 'SUCESS'
        THEN
            Log_Msg( 'Archive for UDA Staging Completed Sucessfully');
        END IF;        

        Log_Msg('****          ARCHIVE STAGING TABLES - COMPLETE            *****');
        Log_Msg('****************************************************************');

    EXCEPTION WHEN OTHERS
    THEN
        lc_msg := 'Unhandled exception in archive_stg_tables. Error: '||SQLCODE||'->'||SQLERRM;
        Log_Msg( lc_msg);
    END archive_stg_tables;    



/* ********************************************************
   * Procedure: purge_if_tables
   *
   * Synopsis: This procedure is to purge interface tables based on the parameter
   *
   * Parameters:
   *   OUT:
   *   IN:
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            22-Mar-2016
   ************************************************************************************* */
    PROCEDURE purge_if_tables (p_purge_if_tables IN VARCHAR2)
    IS
        lc_msg              VARCHAR2(4000) := NULL;
    BEGIN

        Log_Msg('****************************************************************');
        Log_Msg('****         PURGE INTERFACE TABLES - STARTED             *****');

        IF UPPER(p_purge_if_tables) = 'ALL'
        THEN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE INV.MTL_SYSTEM_ITEMS_INTERFACE ';
            EXECUTE IMMEDIATE 'TRUNCATE TABLE INV.MTL_ITEM_REVISIONS_INTERFACE ';
            EXECUTE IMMEDIATE 'TRUNCATE TABLE INV.MTL_INTERFACE_ERRORS ';
            EXECUTE IMMEDIATE 'TRUNCATE TABLE EGO.EGO_ITM_USR_ATTR_INTRFC ';

        ELSIF UPPER(p_purge_if_tables) = 'PROCESSED'
        THEN
            DELETE FROM INV.MTL_INTERFACE_ERRORS WHERE transaction_id IN
            (
                SELECT transaction_id FROM INV.MTL_SYSTEM_ITEMS_INTERFACE      WHERE process_flag = 7
                UNION
                SELECT transaction_id FROM INV.MTL_ITEM_REVISIONS_INTERFACE    WHERE process_flag = 7
                UNION
                SELECT transaction_id FROM EGO_ITM_USR_ATTR_INTRFC             WHERE process_status = 4
            );
            DELETE FROM INV.MTL_SYSTEM_ITEMS_INTERFACE      WHERE process_flag = 7;
            DELETE FROM INV.MTL_ITEM_REVISIONS_INTERFACE    WHERE process_flag = 7;
            DELETE FROM EGO_ITM_USR_ATTR_INTRFC             WHERE process_status = 4;

        ELSIF UPPER(p_purge_if_tables) = 'UNPROCESSED'
        THEN
            DELETE FROM INV.MTL_INTERFACE_ERRORS WHERE transaction_id IN
            (
                SELECT transaction_id FROM INV.MTL_SYSTEM_ITEMS_INTERFACE      WHERE process_flag <> 7
                UNION
                SELECT transaction_id FROM INV.MTL_ITEM_REVISIONS_INTERFACE    WHERE process_flag <> 7
                UNION
                SELECT transaction_id FROM EGO_ITM_USR_ATTR_INTRFC             WHERE process_status <> 4
            );
            DELETE FROM INV.MTL_SYSTEM_ITEMS_INTERFACE      WHERE process_flag <> 7;
            DELETE FROM INV.MTL_ITEM_REVISIONS_INTERFACE    WHERE process_flag <> 7;
            DELETE FROM EGO_ITM_USR_ATTR_INTRFC             WHERE process_status <> 4;

        END IF;

        COMMIT;

        Log_Msg('****         PURGE INTERFACE TABLES - COMPLETED            *****');
        Log_Msg('****************************************************************');

    EXCEPTION WHEN OTHERS
    THEN
        lc_msg := 'Unhandled exception in purge_if_tables. Error: '||SQLCODE||'->'||SQLERRM;
        Log_Msg( lc_msg);
    END purge_if_tables;


    
    
    

/* ********************************************************
   * Procedure: update_batch_id
   *
   * Synopsis: This procedure is to update Batch_ID on the records that are to be processed based on parameter
   *
   * Parameters:
   *   OUT:
   *   IN:
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            17-MAR-2016
   * Narendra Mishra    1.1 Changed logic based on record_status       29-MAR-2016
   * Narendra Mishra    1.2 Changed update logic based on CASE
                            statement and similar_item_number field    25-APR-2016
   ************************************************************************************* */
    PROCEDURE update_batch_id
    IS
        lc_msg          VARCHAR2(4000)  := NULL;
        ln_row_cnt      NUMBER          := 0;
    BEGIN

    -- Added on 25-APR-2016
            UPDATE xxpso_ego_items_stg stg
               SET batch_id             = gn_batch_id,
                   error_message        = NULL
                 WHERE (   CASE WHEN (UPPER(gc_record_status)  = 'NEW') AND (stg.status_code IN (gc_new_flag) )
                                THEN 1
                                WHEN (UPPER(gc_record_status)  = 'FAILED') AND (stg.status_code IN (gc_valid_error_flag, gc_import_error_flag, gc_trans_succ_flag) )
                                THEN 1
                                WHEN (UPPER(gc_record_status)  = 'VALID') AND (stg.status_code IN (gc_valid_succ_flag) )
                                THEN 1
                                WHEN (UPPER(gc_record_status)  = 'ALL') AND (stg.status_code IN (gc_new_flag, gc_valid_succ_flag, gc_valid_error_flag, gc_import_error_flag, gc_trans_succ_flag) )
                                THEN 1
                                ELSE 0
                       END ) = 1
               AND similar_item_number      IS NULL
               AND ROWNUM                   <= DECODE(gn_batch_size, 0, 100000000, gn_batch_size);

            ln_row_cnt  := NVL(SQL%ROWCOUNT,0);

            COMMIT;

            -- Added on 25-APR
            --IF ln_row_cnt < gn_batch_size
            IF ln_row_cnt   = 0
            THEN
                UPDATE xxpso_ego_items_stg stg
                   SET batch_id             = gn_batch_id,
                       error_message        = NULL
                 WHERE (   CASE WHEN (UPPER(gc_record_status)  = 'NEW') AND (stg.status_code IN (gc_new_flag) )
                                THEN 1
                                WHEN (UPPER(gc_record_status)  = 'FAILED') AND (stg.status_code IN (gc_valid_error_flag, gc_import_error_flag, gc_trans_succ_flag) )
                                THEN 1
                                WHEN (UPPER(gc_record_status)  = 'VALID') AND (stg.status_code IN (gc_valid_succ_flag) )
                                THEN 1
                                WHEN (UPPER(gc_record_status)  = 'ALL') AND (stg.status_code IN (gc_new_flag, gc_valid_succ_flag, gc_valid_error_flag, gc_import_error_flag, gc_trans_succ_flag) )
                                THEN 1
                                ELSE 0
                       END ) = 1
                   AND similar_item_number  IS NOT NULL
                   AND ROWNUM               <= DECODE(gn_batch_size, 0, 100000000, gn_batch_size);

                COMMIT;
            END IF;


            UPDATE xxpso_ego_item_attr_stg x
               SET batch_id         = gn_batch_id,
                   error_message    = NULL
                 WHERE (   CASE WHEN (UPPER(gc_record_status)  = 'NEW') AND (x.status_code IN (gc_new_flag) )
                                THEN 1
                                WHEN (UPPER(gc_record_status)  = 'FAILED') AND (x.status_code IN (gc_valid_error_flag, gc_import_error_flag, gc_trans_succ_flag) )
                                THEN 1
                                WHEN (UPPER(gc_record_status)  = 'VALID') AND (x.status_code IN (gc_valid_succ_flag) )
                                THEN 1
                                WHEN (UPPER(gc_record_status)  = 'ALL') AND (x.status_code IN (gc_new_flag, gc_valid_succ_flag, gc_valid_error_flag, gc_import_error_flag, gc_trans_succ_flag) )
                                THEN 1
                                ELSE 0
                       END ) = 1
               AND EXISTS (SELECT 1
                             FROM xxpso_ego_items_stg y
                            WHERE y.source_system       = x.source_system
                              AND y.source_item_number  = x.source_item_number
                              AND y.batch_id            = gn_batch_id
                           );
            COMMIT;

    EXCEPTION WHEN OTHERS
    THEN
        lc_msg := 'Unhandled exception in update_batch_id. Error: '||SQLCODE||'->'||SQLERRM;
        Log_Msg( lc_msg);

        gn_retcode  := gcn_retcode_warning;
    END update_batch_id;

        



   /* ************************************************************************************
   * Procedure: validate_item_data
   *
   * Synopsis: This procedure is to perform custom validations on Item staging table data
   *
   * PARAMETERS:
   *   OUT:
   *   IN:
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            28-JAN-2016
   ************************************************************************************* */
    PROCEDURE validate_item_data
    AS
	    --local variable declaration
	    l_error_flag                VARCHAR2(1)         :=gc_valid_succ_flag;
		l_error_message             VARCHAR2(4000);
        l_organization_id           NUMBER;
        l_inventory_item_id         NUMBER;
        l_source_system_id          NUMBER;
        l_set_process_id            NUMBER;
        l_segment1                  VARCHAR2(200);
        l_prim_uom                  VARCHAR2(50);
        l_catalog_group_id          NUMBER;
        l_item_type                 VARCHAR2(50);
        l_template_id               NUMBER;
        l_template_name             VARCHAR2(50);
        l_attr_char_value           VARCHAR2(1000);
        l_attr_num_value            NUMBER;
        l_attr_date_value           DATE;
        l_transaction_id            NUMBER;
        l_exists                    VARCHAR2(10);
        l_count                     NUMBER;
        l_uda_count                 NUMBER;
        l_uda_idx                   INTEGER             := 1;

	    --Cursor declaration
        CURSOR c_item_data
        IS
            SELECT *
	          FROM xxpso_ego_items_stg
	         WHERE batch_id        = gn_batch_id
	           AND status_code     IN (gc_new_flag, gc_valid_error_flag, gc_import_error_flag, gc_trans_succ_flag);

        CURSOR c_req_attr_data(p_catalog_group VARCHAR2)
        IS
             SELECT eagv.attr_group_name,
                    eagv.attr_group_disp_name,
                    eav.attr_name,
                    eav.attr_display_name,
                    eav.data_type_code
               FROM (SELECT segment1 catalog_group,
                            item_catalog_group_id,
                            parent_catalog_group_id
                        FROM mtl_item_catalog_groups
                       START WITH UPPER(segment1) = UPPER(p_catalog_group)
                     CONNECT BY PRIOR parent_catalog_group_id = item_catalog_group_id
                    ) catl,                     -- SubQuery to get the ICC and it's parent ICCs
                    ego_obj_ag_assocs_b eoaa,
                    xxpso_ego_attr_groups_v eagv,
                    xxpso_ego_attrs_v eav,
                    ego_mappings_b emb,    -- Added to Filter only function generated attributes
                    ego_actions_b eab           -- Added to Filter only function generated attributes
                    --ego_functions_v  efv   -- Added to Filter only function generated attributes
              WHERE TO_CHAR(catl.item_catalog_group_id) = eoaa.CLASSIFICATION_CODE
                AND eoaa.attr_group_id                  = eagv.attr_group_id
                AND eoaa.enabled_flag                   = 'Y'
                AND eagv.application_id                 = eav.application_id
                AND eagv.attr_group_type                = eav.attr_group_type
                AND eagv.attr_group_name                = eav.attr_group_name
                AND eav.enabled_flag                    = 'Y'
                AND emb.mapped_to_group_pk1             = eagv.application_id
                AND emb.mapped_to_group_pk2             = eagv.attr_group_type
                AND emb.mapped_to_group_pk3             = eagv.attr_group_name
                AND emb.mapped_attribute                = eav.attr_name
                AND emb.function_id                     = eab.function_id
                AND eab.action_name                     = 'ItemRequestProcessAction'
                --AND emb.function_id                 = efv.function_id
                --AND UPPER(efv.internal_name)        LIKE 'XX%'
                ;

        TYPE l_item_tab_type IS TABLE OF            c_item_data%ROWTYPE INDEX BY BINARY_INTEGER;
        l_item_tab_tbl                              l_item_tab_type;

        TYPE l_uda_tab_type IS TABLE OF             xxpso_ego_item_attr_stg%ROWTYPE INDEX BY BINARY_INTEGER;
        l_uda_tab_tbl                               l_uda_tab_type;


    BEGIN

        Log_Msg('****************************************************************');
        Log_Msg('****       ITEM STAGING TABLE VALIDATION - STARTED         *****');

        OPEN c_item_data;
        LOOP
            l_item_tab_tbl.DELETE;
            l_uda_tab_tbl.DELETE;
            l_uda_idx   := 1;

            FETCH c_item_data
            BULK COLLECT INTO l_item_tab_tbl LIMIT gcn_item_bulk_limit;

            IF (l_item_tab_tbl.COUNT > 0)
            THEN
                Debug_Msg ('Count of Item data limited to ' || gcn_item_bulk_limit || ' : ' || l_item_tab_tbl.COUNT);

                FOR ln_indx IN l_item_tab_tbl.FIRST .. l_item_tab_tbl.LAST
                LOOP
                    --Initialize the local variables as declared for the procedure
                    l_error_flag        := gc_valid_succ_flag;
                    l_error_message     := NULL;
                    l_exists            := NULL;

                    Debug_Msg('--------------------------------------------------------------------------------------------------------------------------------------------');
                    Debug_Msg('Start of Item Data Validation for Source System - '|| l_item_tab_tbl(ln_indx).source_system || ' Source Item Number ' || l_item_tab_tbl(ln_indx).source_item_number);

                    Debug_Msg(' -- Validation for Duplicates -- ');
                    BEGIN
                        SELECT COUNT(1)
                          INTO l_count
                          FROM xxpso_ego_items_stg
                         WHERE batch_id             = l_item_tab_tbl(ln_indx).batch_id
                           AND source_system        = l_item_tab_tbl(ln_indx).source_system
                           AND source_item_number   = l_item_tab_tbl(ln_indx).source_item_number
                           AND organization_code    = l_item_tab_tbl(ln_indx).organization_code;

                        IF l_count > 1
                        THEN
                            l_error_flag       := gc_valid_error_flag;
                            l_error_message    := l_error_message || 'Duplicate records exists for the same SOURCE_SYSTEM and SOURCE_ITEM_NUMBER ; ';
                            Debug_Msg(l_error_message);

                            CAPTURE_ERROR(   gc_item_ricew_id
                                            ,l_item_tab_tbl(ln_indx).batch_id
                                            ,l_item_tab_tbl(ln_indx).source_system
                                            ,l_item_tab_tbl(ln_indx).source_item_number
                                            ,NULL
                                            ,'SOURCE_SYSTEM - SOURCE_ITEM_NUMBER'
                                            ,l_item_tab_tbl(ln_indx).source_system || ' - ' || l_item_tab_tbl(ln_indx).source_item_number
                                            ,'Duplicate records exists for the same SOURCE_SYSTEM and SOURCE_ITEM_NUMBER'
                                            ,'Contact Functional Team'
                                            ,NULL
                                         );
                        END IF;
                    EXCEPTION
                    WHEN OTHERS THEN
                        NULL;
                    END;


                    Debug_Msg(' -- Validation for Organization Code -- ');
                    BEGIN
                      SELECT organization_id
                        INTO l_item_tab_tbl(ln_indx).organization_id
                        FROM mtl_parameters
                       WHERE organization_code  = l_item_tab_tbl(ln_indx).organization_code;
                    EXCEPTION
                    WHEN OTHERS THEN
                        l_error_flag       := gc_valid_error_flag;
                        l_error_message    := l_error_message || 'ORGANIZATION_CODE - '|| l_item_tab_tbl(ln_indx).organization_code ||' is not defined; ';
                        Debug_Msg(l_error_message);

                        CAPTURE_ERROR(   gc_item_ricew_id
                                        ,l_item_tab_tbl(ln_indx).batch_id
                                        ,l_item_tab_tbl(ln_indx).source_system
                                        ,l_item_tab_tbl(ln_indx).source_item_number
                                        ,NULL
                                        ,'ORGANIZATION_CODE'
                                        ,l_item_tab_tbl(ln_indx).organization_code
                                        ,'Invalid organization code'
                                        ,'Contact Functional Team'
                                        ,NULL
                                     );
                    END;
                    
                    Debug_Msg(' -- Source System Setup and Import Batch validation -- ');
                    BEGIN
                         SELECT eibb.source_system_id, max(eibb.batch_id) set_process_id
                           INTO l_item_tab_tbl(ln_indx).source_system_id, l_item_tab_tbl(ln_indx).set_process_id
                           FROM ego_import_batches_b eibb, hz_orig_systems_vl hosv
                          WHERE eibb.source_system_id   = hosv.orig_system_id
                            AND eibb.batch_status       = 'A'
                            AND hosv.end_date_active    IS NULL
                            AND hosv.status             = 'A'
                            AND hosv.orig_system        = l_item_tab_tbl(ln_indx).source_system
                       GROUP BY hosv.orig_system, eibb.source_system_id;
                    EXCEPTION
                    WHEN OTHERS THEN
                        l_error_flag       := gc_valid_error_flag;
                        l_error_message    := l_error_message || 'Source System in not defined in Oracle; ';
                        Debug_Msg(l_error_message);

                        CAPTURE_ERROR(  gc_item_ricew_id
                                        ,l_item_tab_tbl(ln_indx).batch_id
                                        ,l_item_tab_tbl(ln_indx).source_system
                                        ,l_item_tab_tbl(ln_indx).source_item_number
                                        ,NULL
                                        ,'SOURCE_SYSTEM'
                                        ,l_item_tab_tbl(ln_indx).source_system
                                        ,'Source System in not defined in Oracle'
                                        ,'Contact Functional Team'
                                        ,NULL
                                     );
                    END;                    

                    -- When Similar Item Details are not given
                    IF l_item_tab_tbl(ln_indx).SIMILAR_ITEM_NUMBER IS NULL
                    THEN

                        Debug_Msg(' -- Validation to check if Source System Item Already Exists -- ');
                        BEGIN
                              SELECT mcr.inventory_item_id
                                INTO l_item_tab_tbl(ln_indx).inventory_item_id
                                FROM mtl_cross_references mcr, hz_orig_systems_vl hosv
                               WHERE mcr.source_system_id       = hosv.orig_system_id
                                 AND mcr.cross_reference_type   = 'SS_ITEM_XREF'
                                 AND mcr.end_date_active        IS NULL
                                 AND hosv.end_date_active       IS NULL
                                 AND hosv.status                = 'A'
                                 AND hosv.orig_system           = l_item_tab_tbl(ln_indx).source_system
                                 AND mcr.cross_reference        = l_item_tab_tbl(ln_indx).source_item_number
                                 AND ROWNUM                     = 1;
                        EXCEPTION
                        WHEN OTHERS THEN
                            l_item_tab_tbl(ln_indx).inventory_item_id := NULL;
                        END;

                    -- When Similar Item Details are given
                    ELSE

                        Debug_Msg(' -- Validation to check if Similar Item Already Exists -- ');
                        BEGIN
                              SELECT mcr.inventory_item_id
                                INTO l_item_tab_tbl(ln_indx).inventory_item_id
                                FROM mtl_cross_references mcr, hz_orig_systems_vl hosv
                               WHERE mcr.source_system_id       = hosv.orig_system_id
                                 AND mcr.cross_reference_type   = 'SS_ITEM_XREF'
                                 AND mcr.end_date_active        IS NULL
                                 AND hosv.end_date_active       IS NULL
                                 AND hosv.status                = 'A'
                                 AND hosv.orig_system           = NVL(l_item_tab_tbl(ln_indx).similar_item_source_system, l_item_tab_tbl(ln_indx).source_system)
                                 AND mcr.cross_reference        = l_item_tab_tbl(ln_indx).SIMILAR_ITEM_NUMBER
                                 AND ROWNUM                     = 1;
                        EXCEPTION
                        WHEN OTHERS THEN
                            l_error_flag       := gc_valid_error_flag;
                            l_error_message    := l_error_message || 'Given Similar Item Number is not found in system; ';
                            Debug_Msg(l_error_message);

                            CAPTURE_ERROR(   gc_item_ricew_id
                                            ,l_item_tab_tbl(ln_indx).batch_id
                                            ,l_item_tab_tbl(ln_indx).source_system
                                            ,l_item_tab_tbl(ln_indx).source_item_number
                                            ,NULL
                                            ,'SIMILAR_ITEM_NUMBER'
                                            ,l_item_tab_tbl(ln_indx).SIMILAR_ITEM_NUMBER
                                            ,'Given Similar Item Number is not found in system'
                                            ,'Check Source Data'
                                            ,NULL
                                         );
                        END;

                    END IF;


                    Debug_Msg(' -- Item catalog group validation -- ');
                    BEGIN
                     SELECT item_catalog_group_id
                       INTO l_item_tab_tbl(ln_indx).item_catalog_group_id
                       FROM mtl_item_catalog_groups
                      WHERE enabled_flag                = 'Y'
                        AND item_creation_allowed_flag  = 'Y'
                        AND end_date_active             IS NULL
                        AND UPPER(segment1)             = UPPER(l_item_tab_tbl(ln_indx).item_catalog_group_name);
                    EXCEPTION
                    WHEN OTHERS THEN
                        l_error_flag       := gc_valid_error_flag;
                        l_error_message    := l_error_message || 'Invalid Item catalog group name; ';
                        Debug_Msg(l_error_message);

                        CAPTURE_ERROR(  gc_item_ricew_id
                                        ,l_item_tab_tbl(ln_indx).batch_id
                                        ,l_item_tab_tbl(ln_indx).source_system
                                        ,l_item_tab_tbl(ln_indx).source_item_number
                                        ,NULL
                                        ,'ITEM_CATALOG_GROUP_NAME'
                                        ,l_item_tab_tbl(ln_indx).ITEM_CATALOG_GROUP_NAME
                                        ,'Invalid Item catalog group name'
                                        ,'Check Source Data'
                                        ,NULL
                                        );
                    END;

                    IF l_item_tab_tbl(ln_indx).item_type IS NOT NULL
                    THEN
                        Debug_Msg(' -- Validation of Item Type -- ');
                        BEGIN
                             SELECT lookup_code
                               INTO l_item_tab_tbl(ln_indx).item_type
                               FROM fnd_common_lookups
                              WHERE lookup_type             = 'ITEM_TYPE'
                                AND (UPPER(lookup_code)     = UPPER(l_item_tab_tbl(ln_indx).item_type)
                                 OR  UPPER(meaning)         = UPPER(l_item_tab_tbl(ln_indx).item_type)
                                    );
                        EXCEPTION
                        WHEN OTHERS THEN
                            l_error_flag       := gc_valid_error_flag;
                            l_error_message    := l_error_message || 'Invalid Item Type; ';
                            Debug_Msg(l_error_message);

                            CAPTURE_ERROR(  gc_item_ricew_id
                                            ,l_item_tab_tbl(ln_indx).batch_id
                                            ,l_item_tab_tbl(ln_indx).source_system
                                            ,l_item_tab_tbl(ln_indx).source_item_number
                                            ,NULL
                                            ,'ITEM_TYPE'
                                            ,l_item_tab_tbl(ln_indx).item_type
                                            ,'Invalid Item Type'
                                            ,'Check Source Data'
                                            ,NULL
                                            );
                        END;

                        Debug_Msg(' -- Validation of Template Name validation -- ');
                        BEGIN
                          SELECT mitb.template_id, mitb.template_name
                            INTO l_item_tab_tbl(ln_indx).item_template_id, l_item_tab_tbl(ln_indx).item_template
                            FROM mtl_item_templates_b mitb, mtl_item_templ_attributes mita
                           WHERE mitb.template_id       = mita.template_id
                             AND mita.attribute_name    = 'MTL_SYSTEM_ITEMS.ITEM_TYPE'
                             AND UPPER(mita.attribute_value)   = UPPER(l_item_tab_tbl(ln_indx).item_type);
                        EXCEPTION
                        WHEN OTHERS THEN
                            NULL;
                        END;
                   
                    END IF;
                    
                    

                    Debug_Msg(' -- Validaiton of Item Status -- ');
                    BEGIN
                      SELECT inventory_item_status_code
                        INTO l_item_tab_tbl(ln_indx).inventory_item_status_code
                        FROM mtl_item_status_tl
                       WHERE UPPER(inventory_item_status_code)  = UPPER(l_item_tab_tbl(ln_indx).inventory_item_status_code)
                         AND disable_date                       IS NULL;
                    EXCEPTION
                    WHEN OTHERS THEN
                        l_error_flag       := gc_valid_error_flag;
                        l_error_message    := l_error_message || 'Invalid Item Status; ';
                        Debug_Msg(l_error_message);

                        CAPTURE_ERROR(   gc_item_ricew_id
                                        ,l_item_tab_tbl(ln_indx).batch_id
                                        ,l_item_tab_tbl(ln_indx).source_system
                                        ,l_item_tab_tbl(ln_indx).source_item_number
                                        ,NULL
                                        ,'INVENTORY_ITEM_STATUS_CODE'
                                        ,l_item_tab_tbl(ln_indx).inventory_item_status_code
                                        ,'Invalid Item Status'
                                        ,'Check Source Data'
                                        ,NULL
                                     );
                    END;


                    Debug_Msg(' -- LIFECYCLE validation -- ');
                    BEGIN
                      SELECT PROJ_ELEMENT_ID
                        INTO l_item_tab_tbl(ln_indx).LIFECYCLE_ID
                        FROM PA_EGO_LIFECYCLES_V
                       WHERE OBJECT_TYPE = 'PA_STRUCTURES'
                         AND UPPER(NAME) = UPPER(l_item_tab_tbl(ln_indx).LIFECYCLE);
                    EXCEPTION
                    WHEN OTHERS THEN
                        l_error_flag       := gc_valid_error_flag;
                        l_error_message    := l_error_message || 'Invalid LIFECYCLE; ';
                        Debug_Msg(l_error_message);

                        CAPTURE_ERROR(   gc_item_ricew_id
                                        ,l_item_tab_tbl(ln_indx).batch_id
                                        ,l_item_tab_tbl(ln_indx).source_system
                                        ,l_item_tab_tbl(ln_indx).source_item_number
                                        ,NULL
                                        ,'LIFECYCLE'
                                        ,l_item_tab_tbl(ln_indx).LIFECYCLE
                                        ,'Invalid LIFECYCLE'
                                        ,'Check Source Data'
                                        ,NULL
                                     );
                    END;

                    IF l_item_tab_tbl(ln_indx).LIFECYCLE_ID IS NOT NULL
                    THEN
                        Debug_Msg(' -- LIFECYCLE PHASE validation -- ');
                        BEGIN
                          SELECT PROJ_ELEMENT_ID
                            INTO l_item_tab_tbl(ln_indx).CURRENT_PHASE_ID
                            FROM PA_EGO_LIFECYCLES_PHASES_V
                           WHERE PARENT_STRUCTURE_ID    = l_item_tab_tbl(ln_indx).LIFECYCLE_ID
                             AND UPPER(NAME)            = UPPER(l_item_tab_tbl(ln_indx).LIFECYCLE_PHASE);
                        EXCEPTION
                        WHEN OTHERS THEN
                            l_error_flag       := gc_valid_error_flag;
                            l_error_message    := l_error_message || 'Invalid LIFECYCLE PHASE; ';
                            Debug_Msg(l_error_message);

                            CAPTURE_ERROR(   gc_item_ricew_id
                                            ,l_item_tab_tbl(ln_indx).batch_id
                                            ,l_item_tab_tbl(ln_indx).source_system
                                            ,l_item_tab_tbl(ln_indx).source_item_number
                                            ,NULL
                                            ,'LIFECYCLE_PHASE'
                                            ,l_item_tab_tbl(ln_indx).LIFECYCLE_PHASE
                                            ,'Invalid LIFECYCLE PHASE'
                                            ,'Check Source Data'
                                            ,NULL
                                         );
                        END;

                        Debug_Msg(' -- Checking if Item Status is valid for the given Lifecycle Phase -- ');
                        BEGIN
                         SELECT 'Y'
                           INTO l_exists
                           FROM pa_proj_elements ppe, ego_lcphase_item_status elis
                          WHERE ppe.phase_code = elis.phase_code
                            AND ppe.parent_structure_id         = l_item_tab_tbl(ln_indx).LIFECYCLE_ID
                            AND ppe.proj_element_id             = l_item_tab_tbl(ln_indx).CURRENT_PHASE_ID
                            AND UPPER(elis.item_status_code)    = UPPER(l_item_tab_tbl(ln_indx).inventory_item_status_code)
                            AND ROWNUM                          = 1;
                        EXCEPTION
                        WHEN OTHERS THEN
                            l_error_flag       := gc_valid_error_flag;
                            l_error_message    := l_error_message || 'Item Status is not valid for the given Lifecycle Phase; ';
                            Debug_Msg(l_error_message);

                            CAPTURE_ERROR(   gc_item_ricew_id
                                            ,l_item_tab_tbl(ln_indx).batch_id
                                            ,l_item_tab_tbl(ln_indx).source_system
                                            ,l_item_tab_tbl(ln_indx).source_item_number
                                            ,NULL
                                            ,'INVENTORY_ITEM_STATUS_CODE'
                                            ,l_item_tab_tbl(ln_indx).inventory_item_status_code
                                            ,'Item Status is not valid for the given Lifecycle Phase'
                                            ,'Check Lifecycle Phase Setup'
                                            ,NULL
                                         );
                        END;
                    END IF;


                    Debug_Msg(' -- Primary Unit of Measure validation -- ');
                    BEGIN
                      SELECT unit_of_measure
                        INTO l_item_tab_tbl(ln_indx).primary_unit_of_measure
                        FROM mtl_units_of_measure
                       WHERE disable_date                 IS NULL
                         AND (  UPPER(unit_of_measure)    = UPPER(l_item_tab_tbl(ln_indx).primary_unit_of_measure)
                          OR    UPPER(uom_code)           = UPPER(l_item_tab_tbl(ln_indx).primary_unit_of_measure)
                             );
                    EXCEPTION
                    WHEN OTHERS THEN
                        l_error_flag       := gc_valid_error_flag;
                        l_error_message    := l_error_message || 'Invalid Primary Unit of Measure; ';
                        Debug_Msg(l_error_message);

                        CAPTURE_ERROR(   gc_item_ricew_id
                                        ,l_item_tab_tbl(ln_indx).batch_id
                                        ,l_item_tab_tbl(ln_indx).source_system
                                        ,l_item_tab_tbl(ln_indx).source_item_number
                                        ,NULL
                                        ,'PRIMARY_UNIT_OF_MEASURE'
                                        ,l_item_tab_tbl(ln_indx).primary_unit_of_measure
                                        ,'Invalid Primary Unit of Measure'
                                        ,'Check Source Data'
                                        ,NULL
                                     );
                    END;

                    IF l_item_tab_tbl(ln_indx).dimension_uom IS NOT NULL
                    THEN
                        Debug_Msg(' -- DIMENSION UOM validation -- ');
                        BEGIN
                          SELECT uom_code
                            INTO l_item_tab_tbl(ln_indx).dimension_uom
                            FROM mtl_units_of_measure
                           WHERE uom_class          IN ('Dimension', 'Length')
                             AND disable_date       IS NULL
                             AND (  UPPER(unit_of_measure)    = UPPER(l_item_tab_tbl(ln_indx).dimension_uom)
                              OR    UPPER(uom_code)           = UPPER(l_item_tab_tbl(ln_indx).dimension_uom)
                                 );
                        EXCEPTION
                        WHEN OTHERS THEN
                            l_error_flag       := gc_valid_error_flag;
                            l_error_message    := l_error_message || 'Invalid DIMENSION UOM; ';
                            Debug_Msg(l_error_message);

                            CAPTURE_ERROR(   gc_item_ricew_id
                                            ,l_item_tab_tbl(ln_indx).batch_id
                                            ,l_item_tab_tbl(ln_indx).source_system
                                            ,l_item_tab_tbl(ln_indx).source_item_number
                                            ,NULL
                                            ,'DIMENSION_UOM'
                                            ,l_item_tab_tbl(ln_indx).dimension_uom
                                            ,'Invalid DIMENSION UOM'
                                            ,'Check Source Data'
                                            ,NULL
                                         );
                        END;
                    END IF;

                    IF l_item_tab_tbl(ln_indx).weight_uom IS NOT NULL
                    THEN
                        Debug_Msg(' -- WEIGHT UOM validation -- ');
                        BEGIN
                          SELECT uom_code
                            INTO l_item_tab_tbl(ln_indx).weight_uom
                            FROM mtl_units_of_measure
                           WHERE uom_class          = 'Weight'
                             AND disable_date       IS NULL
                             AND (  UPPER(unit_of_measure)    = UPPER(l_item_tab_tbl(ln_indx).weight_uom)
                              OR    UPPER(uom_code)           = UPPER(l_item_tab_tbl(ln_indx).weight_uom)
                                 );
                        EXCEPTION
                        WHEN OTHERS THEN
                            l_error_flag       := gc_valid_error_flag;
                            l_error_message    := l_error_message || 'Invalid WEIGHT UOM; ';
                            Debug_Msg(l_error_message);

                            CAPTURE_ERROR(   gc_item_ricew_id
                                            ,l_item_tab_tbl(ln_indx).batch_id
                                            ,l_item_tab_tbl(ln_indx).source_system
                                            ,l_item_tab_tbl(ln_indx).source_item_number
                                            ,NULL
                                            ,'WEIGHT_UOM'
                                            ,l_item_tab_tbl(ln_indx).weight_uom
                                            ,'Invalid WEIGHT UOM'
                                            ,'Check Source Data'
                                            ,NULL
                                         );
                        END;
                    END IF;

                    IF l_item_tab_tbl(ln_indx).volume_uom IS NOT NULL
                    THEN
                        Debug_Msg(' -- VOLUME UOM validation -- ');
                        BEGIN
                          SELECT uom_code
                            INTO l_item_tab_tbl(ln_indx).volume_uom
                            FROM mtl_units_of_measure
                           WHERE uom_class          = 'Volume'
                             AND disable_date       IS NULL
                             AND (  UPPER(unit_of_measure)    = UPPER(l_item_tab_tbl(ln_indx).volume_uom)
                              OR    UPPER(uom_code)           = UPPER(l_item_tab_tbl(ln_indx).volume_uom)
                                 );
                        EXCEPTION
                        WHEN OTHERS THEN
                            l_error_flag       := gc_valid_error_flag;
                            l_error_message    := l_error_message || 'Invalid VOLUME UOM; ';
                            Debug_Msg(l_error_message);

                            CAPTURE_ERROR(   gc_item_ricew_id
                                            ,l_item_tab_tbl(ln_indx).batch_id
                                            ,l_item_tab_tbl(ln_indx).source_system
                                            ,l_item_tab_tbl(ln_indx).source_item_number
                                            ,NULL
                                            ,'VOLUME_UOM'
                                            ,l_item_tab_tbl(ln_indx).volume_uom
                                            ,'Invalid VOLUME UOM'
                                            ,'Check Source Data'
                                            ,NULL
                                         );
                        END;
                    END IF;



                    IF l_item_tab_tbl(ln_indx).inventory_item_id IS NULL  -- For New Item Only
                    THEN
                        -- Get all the Function Generated UDA for the given ICC
                        FOR cur_req_attr_rec IN c_req_attr_data(l_item_tab_tbl(ln_indx).item_catalog_group_name)
                        LOOP
                            l_uda_count := 0;
                            l_attr_char_value   := NULL;

                            -- Checking if the UDA already exists for the given Item in UDA Stg Table
                            BEGIN
                                 SELECT COUNT(1)
                                   INTO l_uda_count
                                   FROM xxpso_ego_item_attr_stg
                                  WHERE 1                           = 1
                                    AND source_system               = l_item_tab_tbl(ln_indx).source_system
                                    AND source_item_number          = l_item_tab_tbl(ln_indx).source_item_number
                                    AND item_catalog_group_name     = l_item_tab_tbl(ln_indx).item_catalog_group_name
                                    AND (    UPPER(attr_group_name)        = UPPER(cur_req_attr_rec.attr_group_name)
                                          OR UPPER(attr_group_name)        = UPPER(cur_req_attr_rec.attr_group_disp_name)
                                         )
                                    AND (    UPPER(attr_name)              = UPPER(cur_req_attr_rec.attr_name)
                                          OR UPPER(attr_name)              = UPPER(cur_req_attr_rec.attr_display_name)
                                         )
                                    AND status_code                 IN (gc_new_flag, gc_valid_error_flag, gc_import_error_flag);

                            EXCEPTION
                            WHEN OTHERS THEN
                                l_uda_count := 0;
                            END;

                            IF l_uda_count = 0      -- If UDA is not present in UDA Stg Table
                            THEN
                                --IF UPPER(cur_req_attr_rec.attr_name)            = 'ISBN13'
                                --OR UPPER(cur_req_attr_rec.attr_display_name)    = 'ISBN-13'
                                --THEN
                                    --l_attr_char_value   := l_item_tab_tbl(ln_indx).source_item_number;
                                    --l_attr_char_value       := NULL;

                                --IF UPPER(cur_req_attr_rec.attr_name)            = 'SDESC'
                                --OR UPPER(cur_req_attr_rec.attr_name)            = 'SHORT_DESCRIPTION'
                                --OR UPPER(cur_req_attr_rec.attr_display_name)    = 'SHORT DESCRIPTION'
                                --THEN
                                --    l_attr_char_value   := l_item_tab_tbl(ln_indx).description;
                                --ELSE

                                    l_error_flag       := gc_valid_error_flag;
                                    l_error_message    := l_error_message || 'ICC has Function Generated UDA which is not provided in UDA Staging Table; ';
                                    Debug_Msg(l_error_message);

                                    CAPTURE_ERROR(   gc_item_ricew_id
                                                    ,l_item_tab_tbl(ln_indx).batch_id
                                                    ,l_item_tab_tbl(ln_indx).source_system
                                                    ,l_item_tab_tbl(ln_indx).source_item_number
                                                    ,NULL
                                                    ,'ITEM_CATALOG_GROUP_NAME'
                                                    ,l_item_tab_tbl(ln_indx).item_catalog_group_name
                                                    ,'ICC has Function Generated UDA which is not provided in UDA Staging Table'
                                                    ,'Check Source Data'
                                                    ,NULL
                                                 );
                                --END IF;

                                --IF l_attr_char_value IS NOT NULL    -- If UDA value is Null
                                --THEN
                                --    -- Insert record in UDA Stg Table
                                --    BEGIN
                                --        l_uda_tab_tbl(l_uda_idx).record_id                  := XXPSO_EGO_ITEM_ATTR_S.NEXTVAL;
                                --        l_uda_tab_tbl(l_uda_idx).batch_id                   := l_item_tab_tbl(ln_indx).batch_id;
                                --        l_uda_tab_tbl(l_uda_idx).item_record_id	            := l_item_tab_tbl(ln_indx).record_id;
                                --        l_uda_tab_tbl(l_uda_idx).source_system              := l_item_tab_tbl(ln_indx).source_system;
                                --        l_uda_tab_tbl(l_uda_idx).source_item_number         := l_item_tab_tbl(ln_indx).source_item_number;
                                --        l_uda_tab_tbl(l_uda_idx).organization_code          := l_item_tab_tbl(ln_indx).organization_code;
                                --        l_uda_tab_tbl(l_uda_idx).item_catalog_group_name    := l_item_tab_tbl(ln_indx).item_catalog_group_name;
                                --        l_uda_tab_tbl(l_uda_idx).attr_group_name            := cur_req_attr_rec.attr_group_name;
                                --        l_uda_tab_tbl(l_uda_idx).attr_name                  := cur_req_attr_rec.attr_name;
                                --        l_uda_tab_tbl(l_uda_idx).attr_char_value            := SUBSTR(l_attr_char_value,1,240);
                                --        l_uda_tab_tbl(l_uda_idx).attr_num_value             := l_attr_num_value;
                                --        l_uda_tab_tbl(l_uda_idx).attr_date_value            := l_attr_date_value;
                                --        l_uda_tab_tbl(l_uda_idx).row_identifier             := 100;
                                --        l_uda_tab_tbl(l_uda_idx).status_code                := 'N';
                                --        l_uda_tab_tbl(l_uda_idx).created_by                 := fnd_global.user_id;
                                --        l_uda_tab_tbl(l_uda_idx).creation_date              := SYSDATE;
                                --
                                --        l_uda_idx   := l_uda_idx + 1;
                                --    EXCEPTION
                                --    WHEN OTHERS THEN
                                --        NULL;
                                --    END;
                                --
                                --END IF; -- l_attr_char_value IS NOT NULL

                            END IF;  -- l_uda_count = 0

                        END LOOP;

                    END IF; -- l_item_tab_tbl(ln_indx).inventory_item_id IS NULL


                    IF l_error_flag = gc_valid_succ_flag
                    THEN
                        l_item_tab_tbl(ln_indx).status_code         := l_error_flag;
                        l_item_tab_tbl(ln_indx).error_message       := l_error_message;

                        -- Generating Transaction ID
                        l_item_tab_tbl(ln_indx).transaction_id      := MTL_SYSTEM_ITEMS_INTERFACE_S.NEXTVAL;

                        Debug_Msg('All the validation is successful for the record');

                    ELSE
                        l_item_tab_tbl(ln_indx).status_code         := l_error_flag;
                        l_item_tab_tbl(ln_indx).error_message       := l_error_message;

                        gn_retcode  := gcn_retcode_warning;
                        Debug_Msg('Error in validation for the record');

                    END IF;


/*                     IF l_item_tab_tbl(ln_indx).inventory_item_id IS NOT NULL
                    THEN
                        l_item_tab_tbl(ln_indx).status_code         := gc_valid_error_flag;
                        l_item_tab_tbl(ln_indx).error_message       := 'XXX';
                    END IF;    */


                END LOOP;

                -----------------------------------------------------------
                -- Updating the Item Staging Table
                -----------------------------------------------------------
                BEGIN
                    FORALL i IN INDICES OF l_item_tab_tbl SAVE EXCEPTIONS
                         UPDATE xxpso_ego_items_stg
                            SET status_code                 = l_item_tab_tbl(i).status_code
                               ,error_message               = l_item_tab_tbl(i).error_message
                               ,organization_id             = l_item_tab_tbl(i).organization_id
                               ,inventory_item_id           = l_item_tab_tbl(i).inventory_item_id
                               ,source_system_id            = l_item_tab_tbl(i).source_system_id
                               ,set_process_id              = l_item_tab_tbl(i).set_process_id
                               ,item_catalog_group_id       = l_item_tab_tbl(i).item_catalog_group_id
                               ,item_type                   = l_item_tab_tbl(i).item_type
                               ,item_template               = l_item_tab_tbl(i).item_template
                               ,item_template_id            = l_item_tab_tbl(i).item_template_id
                               ,inventory_item_status_code  = l_item_tab_tbl(i).inventory_item_status_code
                               ,lifecycle_id                = l_item_tab_tbl(i).lifecycle_id
                               ,current_phase_id            = l_item_tab_tbl(i).current_phase_id
                               ,primary_unit_of_measure     = l_item_tab_tbl(i).primary_unit_of_measure
                               ,dimension_uom               = l_item_tab_tbl(i).dimension_uom
                               ,weight_uom                  = l_item_tab_tbl(i).weight_uom
                               ,volume_uom                  = l_item_tab_tbl(i).volume_uom
                               ,transaction_id              = l_item_tab_tbl(i).transaction_id
                               ,last_update_date            = SYSDATE
                               ,last_updated_by             = gcn_user_id
                               ,conc_request_id             = gcn_request_id
                          WHERE record_id                   = l_item_tab_tbl(i).record_id;
                    COMMIT;
                EXCEPTION
                    WHEN OTHERS THEN
                         Debug_Msg('Error while updating Item staging table.');
                END;

                -----------------------------------------------------------
                -- Insert UDA records in UDA Staging Table
                -----------------------------------------------------------
                --BEGIN
                --    FORALL i IN INDICES OF l_uda_tab_tbl SAVE EXCEPTIONS
                --        INSERT INTO xxpso_ego_item_attr_stg
                --        VALUES l_uda_tab_tbl(i);
                --
                --    COMMIT;
                --EXCEPTION
                --    WHEN OTHERS THEN
                --         Debug_Msg('Error while Inserting Into UDA staging table.');
                --END;

            END IF;

            EXIT WHEN c_item_data%NOTFOUND;

        END LOOP;

        CLOSE c_item_data;

        Log_Msg('****       ITEM STAGING TABLE VALIDATION - COMPLETED       *****');
        Log_Msg('****************************************************************');

    EXCEPTION
        WHEN OTHERS THEN
            l_error_message := 'Unexpected Error in validate_item_data procedure. SQLERRM : ' || SQLERRM;
            Log_Msg(l_error_message);

            gn_retcode  := gcn_retcode_warning;
    END validate_item_data;



   /* ************************************************************************************
   * Procedure: validate_uda_data
   *
   * Synopsis: This procedure is to perform custom validations on UDA staging table data
   *
   * PARAMETERS:
   *   OUT:
   *   IN:
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            28-JAN-2016
   ************************************************************************************* */
    PROCEDURE validate_uda_data
    AS
	    --local variable declaration
	    l_error_flag                VARCHAR2(1)         :=gc_valid_succ_flag;
		l_error_message             VARCHAR2(4000);
        l_organization_id           NUMBER;
        l_inventory_item_id         NUMBER;
        l_source_system_id          NUMBER;
        l_set_process_id            NUMBER;
        l_transaction_id            NUMBER;
        l_segment1                  VARCHAR2(200);
        l_prim_uom                  VARCHAR2(50);
        l_catalog_group_id          NUMBER;
        l_attr_group_id             NUMBER;
        l_attr_group_name           VARCHAR2(100);
        l_attr_id                   NUMBER;
        l_attr_name                 VARCHAR2(100);
        l_data_type_code            VARCHAR2(10);
        l_attr_group_type           VARCHAR2(100);
        l_data_level_id             NUMBER;
        l_exists                    VARCHAR2(10);

        l_value_set_id              NUMBER;
        l_value_set_name            VARCHAR2(50);
        l_validation_type           VARCHAR2(2);
        l_vset_value                VARCHAR2(1000);
        l_vset_value_count          NUMBER;
        l_item_status_code          VARCHAR2(2);

	    --Cursor declaration
        CURSOR c_uda_data
        IS
            SELECT *
	          FROM xxpso_ego_item_attr_stg
	         WHERE batch_id         = gn_batch_id
	           --AND status_code      = gc_new_flag;
              AND status_code       IN (gc_new_flag, gc_valid_error_flag, gc_import_error_flag, gc_trans_succ_flag);

        TYPE l_uda_tab_type IS TABLE OF             c_uda_data%ROWTYPE INDEX BY BINARY_INTEGER;
        l_uda_tab_tbl                               l_uda_tab_type;

    BEGIN

        Log_Msg('****************************************************************');
        Log_Msg('****        UDA STAGING TABLE VALIDATION - STARTED         *****');

        /* UPDATE xxpso_ego_item_attr_stg x
           SET x.status_code        = gc_new_flag,
               x.error_message      = NULL
         WHERE x.batch_id           = gn_batch_id
           AND x.status_code        IN (gc_valid_error_flag, gc_import_error_flag, gc_trans_succ_flag);

        -- Updating Item Record ID from Item Stg Table
        UPDATE xxpso_ego_item_attr_stg x
           SET x.item_record_id     = (SELECT MIN(record_id)
                                         FROM xxpso_ego_items_stg y
                                        WHERE y.source_system       = x.source_system
                                          AND y.source_item_number  = x.source_item_number
                                          AND y.status_code         IN (gc_new_flag, gc_valid_error_flag, gc_import_error_flag, gc_valid_succ_flag)
                                       )
         WHERE x.batch_id           = gn_batch_id
           AND x.status_code        = gc_new_flag
           AND x.item_record_id     IS NULL;

        -- Erroring UDA Records for which Item Records are Invalid
        UPDATE xxpso_ego_item_attr_stg x
           SET x.status_code        = gc_valid_error_flag
              ,x.error_message      = 'Corresponding Item Record in Invalid in Item Staging Table'
              ,last_update_date     = SYSDATE
              ,last_updated_by      = gcn_user_id
              ,conc_request_id      = gcn_request_id
         WHERE x.batch_id           = gn_batch_id
           AND x.status_code        = gc_new_flag
           AND EXISTS (SELECT 1
                         FROM xxpso_ego_items_stg y
                        WHERE y.source_system       = x.source_system
                          AND y.source_item_number  = x.source_item_number
                          AND y.status_code         = gc_valid_error_flag
                      );
        COMMIT;   */


        OPEN c_uda_data;
        LOOP
            l_uda_tab_tbl.DELETE;

            FETCH c_uda_data
            BULK COLLECT INTO l_uda_tab_tbl LIMIT gcn_uda_bulk_limit;

            IF (l_uda_tab_tbl.COUNT > 0)
            THEN
                Debug_Msg ('Count of UDA data limited to ' || gcn_uda_bulk_limit || ' : ' || l_uda_tab_tbl.COUNT);

                FOR ln_indx IN l_uda_tab_tbl.FIRST .. l_uda_tab_tbl.LAST
                LOOP
                    --Initialize the local variables as declared for the procedure
                    l_error_flag        := gc_valid_succ_flag;
                    l_error_message     := NULL;
                    l_exists            := NULL;
                    l_value_set_id      := NULL;
                    l_value_set_name    := NULL;
                    l_validation_type   := NULL;
                    l_vset_value        := NULL;
                    l_vset_value_count  := 0;
                    l_item_status_code  := NULL;


                    Debug_Msg('-----------------------------------------------------------------------------');
                    Debug_Msg('Start of UDA Data Validation for Source System - '|| l_uda_tab_tbl(ln_indx).source_system || ' Source Item Number ' || l_uda_tab_tbl(ln_indx).source_item_number);

                    Debug_Msg(' -- Checking if Item exists in Item Staging Table -- ');
                    BEGIN
                          SELECT inventory_item_id, organization_id, transaction_id, source_system_id, set_process_id, record_id, status_code
                            INTO l_uda_tab_tbl(ln_indx).inventory_item_id, l_uda_tab_tbl(ln_indx).organization_id, l_uda_tab_tbl(ln_indx).transaction_id,
                                 l_uda_tab_tbl(ln_indx).source_system_id, l_uda_tab_tbl(ln_indx).set_process_id, l_uda_tab_tbl(ln_indx).item_record_id, l_item_status_code
                            FROM xxpso_ego_items_stg
                           WHERE batch_id                   = l_uda_tab_tbl(ln_indx).batch_id
                             AND source_system              = l_uda_tab_tbl(ln_indx).source_system
                             AND source_item_number         = l_uda_tab_tbl(ln_indx).source_item_number
                             --AND status_code                IN (gc_valid_succ_flag, gc_trans_succ_flag)
                             ;
                    EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        l_uda_tab_tbl(ln_indx).transaction_id    := NULL;
                    WHEN OTHERS THEN
                        l_uda_tab_tbl(ln_indx).transaction_id    := NULL;
                    END;

                    IF l_uda_tab_tbl(ln_indx).transaction_id IS NULL     --- IF ITEM DOES NOT EXISTS IN ITEM STG TABLE
                    THEN
                        l_error_flag       := gc_valid_error_flag;
                        l_error_message    := l_error_message || 'Item does not exist in Item Staging Table; ';
                        Debug_Msg(l_error_message);

                        CAPTURE_ERROR(  gc_uda_ricew_id
                                        ,l_uda_tab_tbl(ln_indx).batch_id
                                        ,l_uda_tab_tbl(ln_indx).source_system
                                        ,l_uda_tab_tbl(ln_indx).source_item_number
                                        ,NULL
                                        ,'SOURCE_ITEM_NUMBER'
                                        ,l_uda_tab_tbl(ln_indx).source_item_number
                                        ,'Item does not exist in Item Staging Table'
                                        ,'Check Source Data'
                                        ,NULL
                                     );
                    ELSIF l_item_status_code <> gc_valid_succ_flag
                    THEN
                        l_error_flag       := gc_valid_error_flag;
                        l_error_message    := l_error_message || 'Item record is Invalid in Item Staging Table; ';
                        Debug_Msg(l_error_message);

                        CAPTURE_ERROR(  gc_uda_ricew_id
                                        ,l_uda_tab_tbl(ln_indx).batch_id
                                        ,l_uda_tab_tbl(ln_indx).source_system
                                        ,l_uda_tab_tbl(ln_indx).source_item_number
                                        ,NULL
                                        ,'SOURCE_ITEM_NUMBER'
                                        ,l_uda_tab_tbl(ln_indx).source_item_number
                                        ,'Item record is Invalid in Item Staging Table'
                                        ,'Check Source Data'
                                        ,NULL
                                     );
                    END IF;

                    Debug_Msg(' -- ITEM CATALOG GROUP NAME validation -- ');
                    BEGIN
                     SELECT item_catalog_group_id
                       INTO l_uda_tab_tbl(ln_indx).item_catalog_group_id
                       FROM mtl_item_catalog_groups
                      WHERE enabled_flag                = 'Y'
                        AND item_creation_allowed_flag  = 'Y'
                        AND end_date_active             IS NULL
                        AND segment1                    = l_uda_tab_tbl(ln_indx).item_catalog_group_name;
                    EXCEPTION
                    WHEN OTHERS THEN
                        l_error_flag       := gc_valid_error_flag;
                        l_error_message    := l_error_message || 'Invalid Item catalog group name; ';
                        Debug_Msg(l_error_message);

                        CAPTURE_ERROR(  gc_uda_ricew_id
                                        ,l_uda_tab_tbl(ln_indx).batch_id
                                        ,l_uda_tab_tbl(ln_indx).source_system
                                        ,l_uda_tab_tbl(ln_indx).source_item_number
                                        ,NULL
                                        ,'ITEM_CATALOG_GROUP_NAME'
                                        ,l_uda_tab_tbl(ln_indx).ITEM_CATALOG_GROUP_NAME
                                        ,'Invalid Item catalog group name'
                                        ,'Check Source Data'
                                        ,NULL
                                        );
                    END;


                    Debug_Msg(' -- Attribute Group Validation -- ');
                    BEGIN
                      SELECT eagv.attr_group_id, eagv.attr_group_name, eagv.attr_group_type, eagd.data_level_id
                        INTO l_uda_tab_tbl(ln_indx).attr_group_id, l_uda_tab_tbl(ln_indx).attr_group_name, l_uda_tab_tbl(ln_indx).attr_group_type, l_uda_tab_tbl(ln_indx).data_level_id
                        FROM xxpso_ego_attr_groups_v eagv, ego_attr_group_dl eagd
                       WHERE eagv.application_id                    = 431
                         AND eagv.attr_group_id                     = eagd.attr_group_id
                         AND UPPER(eagv.attr_group_name)         = UPPER(l_uda_tab_tbl(ln_indx).ATTR_GROUP_NAME);
                          --OR    UPPER(eagv.attr_group_disp_name)    = UPPER(l_uda_tab_tbl(ln_indx).ATTR_GROUP_NAME)

                    EXCEPTION
                    WHEN OTHERS THEN
                        l_error_flag       := gc_valid_error_flag;
                        l_error_message    := l_error_message || 'ATTR_GROUP_NAME - '|| l_uda_tab_tbl(ln_indx).ATTR_GROUP_NAME ||' is not defined; ';
                        Debug_Msg(l_error_message);

                        CAPTURE_ERROR(   gc_uda_ricew_id
                                        ,l_uda_tab_tbl(ln_indx).batch_id
                                        ,l_uda_tab_tbl(ln_indx).source_system
                                        ,l_uda_tab_tbl(ln_indx).source_item_number
                                        ,NULL
                                        ,'ATTR_GROUP_NAME'
                                        ,l_uda_tab_tbl(ln_indx).ATTR_GROUP_NAME
                                        ,'Invalid Attribute Group'
                                        ,'Check Source Data'
                                        ,NULL
                                     );
                    END;

                    IF l_uda_tab_tbl(ln_indx).attr_group_id IS NOT NULL AND l_uda_tab_tbl(ln_indx).item_catalog_group_id IS NOT NULL
                    THEN

                        Debug_Msg(' -- Checking if the Attribute Group has been assigned to the given ICC -- ');
                        BEGIN
                             SELECT 'Y'
                               INTO l_exists
                               FROM  (SELECT segment1 catalog_group,
                                             item_catalog_group_id,
                                             parent_catalog_group_id
                                        FROM mtl_item_catalog_groups
                                       START WITH item_catalog_group_id         = l_uda_tab_tbl(ln_indx).item_catalog_group_id
                                     CONNECT BY PRIOR parent_catalog_group_id   = item_catalog_group_id
                                     ) catl,
                                    ego_obj_ag_assocs_b eoaa
                              WHERE TO_CHAR(catl.item_catalog_group_id) = eoaa.CLASSIFICATION_CODE
                                AND eoaa.attr_group_id                  = l_uda_tab_tbl(ln_indx).attr_group_id
                                AND ROWNUM                              = 1;
                        EXCEPTION
                        WHEN OTHERS THEN
                            l_error_flag       := gc_valid_error_flag;
                            l_error_message    := l_error_message ||' Attribute Group - '||l_uda_tab_tbl(ln_indx).ATTR_GROUP_NAME||' is not assigned to the ICC - '|| l_uda_tab_tbl(ln_indx).item_catalog_group_name;
                            Debug_Msg(l_error_message);

                            CAPTURE_ERROR(   gc_uda_ricew_id
                                            ,l_uda_tab_tbl(ln_indx).batch_id
                                            ,l_uda_tab_tbl(ln_indx).source_system
                                            ,l_uda_tab_tbl(ln_indx).source_item_number
                                            ,NULL
                                            ,'ITEM_CATALOG_GROUP_NAME - ATTR_GROUP_NAME'
                                            ,l_uda_tab_tbl(ln_indx).item_catalog_group_name ||' - '||l_uda_tab_tbl(ln_indx).ATTR_GROUP_NAME
                                            ,'Attribute Group is not assigned to the ICC'
                                            ,'Check ICC Setup'
                                            ,NULL
                                         );
                        END;


                        Debug_Msg(' -- Attribute Validation -- ');
                        BEGIN
                          SELECT attr_id, attr_name, data_type_code, value_set_id
                            INTO l_uda_tab_tbl(ln_indx).attr_id, l_uda_tab_tbl(ln_indx).attr_name, l_uda_tab_tbl(ln_indx).data_type_code, l_value_set_id
                            FROM xxpso_ego_attrs_v
                           WHERE application_id             = 431
                             AND enabled_flag               = 'Y'
                             AND UPPER(attr_group_name)     = UPPER(l_uda_tab_tbl(ln_indx).attr_group_name)
                             AND UPPER(attr_name)           = UPPER(l_uda_tab_tbl(ln_indx).attr_name);
                              --OR   UPPER(attr_display_name) = UPPER(l_uda_tab_tbl(ln_indx).attr_name)
                              --   );
                        EXCEPTION
                        WHEN OTHERS THEN
                            l_error_flag       := gc_valid_error_flag;
                            l_error_message    := l_error_message || 'ATTR_GROUP_NAME.ATTR_NAME - '||l_uda_tab_tbl(ln_indx).ATTR_GROUP_NAME || '.' || l_uda_tab_tbl(ln_indx).attr_name ||' is not defined; ';
                            Debug_Msg(l_error_message);

                            CAPTURE_ERROR(   gc_uda_ricew_id
                                            ,l_uda_tab_tbl(ln_indx).batch_id
                                            ,l_uda_tab_tbl(ln_indx).source_system
                                            ,l_uda_tab_tbl(ln_indx).source_item_number
                                            ,NULL
                                            ,'ATTR_NAME'
                                            ,l_uda_tab_tbl(ln_indx).attr_name
                                            ,'Invalid Attribute'
                                            ,'Check Source Data'
                                            ,NULL
                                         );
                        END;

                        IF l_uda_tab_tbl(ln_indx).attr_id IS NOT NULL
                        THEN

                            Debug_Msg(' -- Data Type and Corresponding Value Field Validation -- ');
                            IF l_uda_tab_tbl(ln_indx).data_type_code = 'C' AND l_uda_tab_tbl(ln_indx).ATTR_CHAR_VALUE IS NULL
                            THEN
                               --IF l_uda_tab_tbl(ln_indx).ATTR_NUM_VALUE IS NOT NULL
                               --THEN
                               --    l_uda_tab_tbl(ln_indx).ATTR_CHAR_VALUE  := TO_CHAR(l_uda_tab_tbl(ln_indx).ATTR_NUM_VALUE);
                               --    l_uda_tab_tbl(ln_indx).ATTR_NUM_VALUE   := NULL;
                               --ELSE

                                l_error_flag       := gc_valid_error_flag;
                                l_error_message    := l_error_message || 'Data Type of Attribute is Char but value of ATTR_CHAR_VALUE is missing; ';
                                Debug_Msg(l_error_message);
                                CAPTURE_ERROR(   gc_uda_ricew_id
                                                ,l_uda_tab_tbl(ln_indx).batch_id
                                                ,l_uda_tab_tbl(ln_indx).source_system
                                                ,l_uda_tab_tbl(ln_indx).source_item_number
                                                ,NULL
                                                ,'ATTR_NAME'
                                                ,l_uda_tab_tbl(ln_indx).attr_name
                                                ,'Data Type of Attribute is Char but value of ATTR_CHAR_VALUE is missing'
                                                ,'Check Source Data'
                                                ,NULL
                                             );
                                --END IF;

                            ELSIF l_uda_tab_tbl(ln_indx).data_type_code = 'N' AND l_uda_tab_tbl(ln_indx).ATTR_NUM_VALUE IS NULL
                            THEN
                                l_error_flag       := gc_valid_error_flag;
                                l_error_message    := l_error_message || 'Data Type of Attribute is Number but value of ATTR_NUM_VALUE is missing; ';
                                Debug_Msg(l_error_message);
                                CAPTURE_ERROR(   gc_uda_ricew_id
                                                ,l_uda_tab_tbl(ln_indx).batch_id
                                                ,l_uda_tab_tbl(ln_indx).source_system
                                                ,l_uda_tab_tbl(ln_indx).source_item_number
                                                ,NULL
                                                ,'ATTR_NAME'
                                                ,l_uda_tab_tbl(ln_indx).attr_name
                                                ,'Data Type of Attribute is Number but value of ATTR_NUM_VALUE is missing'
                                                ,'Check Source Data'
                                                ,NULL
                                             );
                            ELSIF l_uda_tab_tbl(ln_indx).data_type_code = 'X' AND l_uda_tab_tbl(ln_indx).ATTR_DATE_VALUE IS NULL
                            THEN
                                l_error_flag       := gc_valid_error_flag;
                                l_error_message    := l_error_message || 'Data Type of Attribute is Date but value of ATTR_DATE_VALUE is missing; ';
                                Debug_Msg(l_error_message);
                                CAPTURE_ERROR(   gc_uda_ricew_id
                                                ,l_uda_tab_tbl(ln_indx).batch_id
                                                ,l_uda_tab_tbl(ln_indx).source_system
                                                ,l_uda_tab_tbl(ln_indx).source_item_number
                                                ,NULL
                                                ,'ATTR_NAME'
                                                ,l_uda_tab_tbl(ln_indx).attr_name
                                                ,'Data Type of Attribute is Date but value of ATTR_DATE_VALUE is missing'
                                                ,'Check Source Data'
                                                ,NULL
                                             );
                            END IF;

                            -- Added this change for SIT/7i only for the Bug#MDMCP-2286
                            -- Commented on 10-Apr-2015
/*                             IF ( UPPER(l_uda_tab_tbl(ln_indx).attr_group_name) = 'PSO_CONTRIBUTOR' AND UPPER(l_uda_tab_tbl(ln_indx).attr_name) = 'AFFILIATION' )
                            OR ( UPPER(l_uda_tab_tbl(ln_indx).attr_group_name) = 'PSO_FILE_PROPERTIES' AND UPPER(l_uda_tab_tbl(ln_indx).attr_name) = 'DESCRIPTION' )
                            THEN
                                l_uda_tab_tbl(ln_indx).ATTR_CHAR_VALUE  := SUBSTR(l_uda_tab_tbl(ln_indx).ATTR_CHAR_VALUE,1,150);
                            END IF;  */


                        END IF; -- IF l_attr_id IS NOT NULL

                    END IF; -- IF l_attr_group_id IS NOT NULL

                    -- Added on 23-Mar-2015
                    -- Commented on 29-Mar-2015
                    /* IF l_error_flag = gc_valid_succ_flag AND l_value_set_id IS NOT NULL
                    THEN
                        BEGIN
                            -- Checking if Value Set is Independent Type
                          SELECT flex_value_set_name, validation_type
                            INTO l_value_set_name, l_validation_type
                            FROM fnd_flex_value_sets
                           WHERE flex_value_set_id  = l_value_set_id;

                            -- If Independent Value Set
                            IF l_validation_type = 'I'
                            THEN
                                l_vset_value   := NVL(l_uda_tab_tbl(ln_indx).ATTR_CHAR_VALUE, NVL(TO_CHAR(l_uda_tab_tbl(ln_indx).ATTR_NUM_VALUE), TO_CHAR(l_uda_tab_tbl(ln_indx).ATTR_DATE_VALUE,'YYYY/MM/DD')));

                              -- Checking if Value Already Exists
                              SELECT COUNT(1)
                                INTO l_vset_value_count
                                FROM fnd_flex_values
                               WHERE flex_value_set_id      = l_value_set_id
                                 AND flex_value             = l_vset_value;

                                -- If Not Present, creating the value
                                IF l_vset_value_count = 0
                                THEN
                                    IF create_vset_value(l_value_set_name, l_vset_value)
                                    THEN
                                        Log_Msg('Create Value : '|| l_vset_value || ' in the value set : '|| l_value_set_name);
                                    END IF;
                                END IF;
                            END IF;
                        END;
                    END IF; */


                    l_uda_tab_tbl(ln_indx).status_code         := l_error_flag;
                    l_uda_tab_tbl(ln_indx).error_message       := l_error_message;

/*                     IF l_uda_tab_tbl(ln_indx).inventory_item_id IS NOT NULL
                    THEN
                        l_uda_tab_tbl(ln_indx).status_code         := gc_valid_error_flag;
                        l_uda_tab_tbl(ln_indx).error_message       := 'XXX';
                    END IF;   */

                    IF l_error_flag = gc_valid_succ_flag
                    THEN
                        Debug_Msg('All the validation is successful');
                    ELSE
                        Debug_Msg('Error in validation');
                        gn_retcode  := gcn_retcode_warning;
                    END IF;

                END LOOP;

                -----------------------------------------------------------
                -- Updating the Staging Table
                -----------------------------------------------------------
                BEGIN
                     FORALL i IN INDICES OF l_uda_tab_tbl SAVE EXCEPTIONS
                     UPDATE xxpso_ego_item_attr_stg
                        SET status_code                 = l_uda_tab_tbl(i).status_code
                           ,error_message               = l_uda_tab_tbl(i).error_message
                           ,item_record_id              = l_uda_tab_tbl(i).item_record_id
                           ,organization_id             = l_uda_tab_tbl(i).organization_id
                           ,source_system_id            = l_uda_tab_tbl(i).source_system_id
                           ,set_process_id              = l_uda_tab_tbl(i).set_process_id
                           ,inventory_item_id           = l_uda_tab_tbl(i).inventory_item_id
                           ,transaction_id              = l_uda_tab_tbl(i).transaction_id
                           ,item_catalog_group_id       = l_uda_tab_tbl(i).item_catalog_group_id
                           ,attr_group_id               = l_uda_tab_tbl(i).attr_group_id
                           ,attr_group_name             = l_uda_tab_tbl(i).attr_group_name
                           ,data_level_id               = l_uda_tab_tbl(i).data_level_id
                           ,attr_id                     = l_uda_tab_tbl(i).attr_id
                           ,attr_name                   = l_uda_tab_tbl(i).attr_name
                           ,data_type_code              = l_uda_tab_tbl(i).data_type_code
                           ,attr_group_type             = l_uda_tab_tbl(i).attr_group_type
                           ,attr_char_value             = l_uda_tab_tbl(i).attr_char_value
                           ,last_update_date            = SYSDATE
                           ,last_updated_by             = gcn_user_id
                           ,conc_request_id             = gcn_request_id
                      WHERE record_id                   = l_uda_tab_tbl(i).record_id;

                    COMMIT;
                EXCEPTION
                    WHEN OTHERS THEN
                         Debug_Msg('Error while updating staging table.');
                END;

            END IF;

            EXIT WHEN c_uda_data%NOTFOUND;

        END LOOP;

        CLOSE c_uda_data;

        Log_Msg('****        UDA STAGING TABLE VALIDATION - COMPLETED       *****');
        Log_Msg('****************************************************************');

    EXCEPTION
        WHEN OTHERS THEN
            l_error_message := 'Unexpected Error in validate_uda_data procedure. SQLERRM : ' || SQLERRM;
            Log_Msg(l_error_message);

            gn_retcode  := gcn_retcode_warning;
    END validate_uda_data;




   /* ************************************************************************************
   * Procedure: partition_data
   *
   * Synopsis: This procedure is to partition staging data by different Import Batch ID based on parameter
   *
   * PARAMETERS:
   *   OUT:
   *   IN:
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            21-Mar-2016
   ************************************************************************************* */
    PROCEDURE partition_data
    AS
	    --local variable declaration
	    l_error_flag                    VARCHAR2(1)         :=gc_trans_succ_flag;
		l_error_message                 VARCHAR2(4000);
        l_new_set_process_id            NUMBER              := -1;
        l_partition_size                NUMBER              := 0;
        l_record_no                     NUMBER              := 1;
        ln_batch_indx                   NUMBER              := 1;

	    --Cursor declaration
        CURSOR c_ss_data
        IS
             SELECT DISTINCT source_system_id
	           FROM xxpso_ego_items_stg
	          WHERE batch_id         = gn_batch_id
	            AND status_code      IN (gc_valid_succ_flag, gc_import_error_flag)
                AND transaction_id   IS NOT NULL;

        CURSOR c_item_data (p_source_system_id NUMBER)
        IS
             SELECT record_id, set_process_id, transaction_id
	           FROM xxpso_ego_items_stg
	          WHERE batch_id         = gn_batch_id
                AND status_code      IN (gc_valid_succ_flag, gc_import_error_flag)
                AND transaction_id   IS NOT NULL
                AND source_system_id = p_source_system_id;

        CURSOR c_batch (p_source_system_id NUMBER)
        IS
             SELECT eibb.batch_id
               FROM ego_import_batches_b eibb
              WHERE eibb.batch_status       = 'A'
                AND eibb.source_system_id   = p_source_system_id
           ORDER BY eibb.batch_id DESC;

        TYPE l_item_tab_type IS TABLE OF            c_item_data%ROWTYPE INDEX BY BINARY_INTEGER;
        l_item_tab_tbl                              l_item_tab_type;

        TYPE l_batch_tab_type IS TABLE OF           c_batch%ROWTYPE INDEX BY BINARY_INTEGER;
        l_batch_tbl                                 l_batch_tab_type;

    BEGIN

        Log_Msg('****************************************************************');
        Log_Msg('***  SETTING THE DATA SET ID FOR MULTI THREADING - STARTED   ***');

        FOR c IN c_ss_data
        LOOP

            Log_Msg('source_system_id       : '||c.source_system_id);

            -- Getting all the existing batches for Source System
            OPEN c_batch(c.source_system_id);
                l_batch_tbl.DELETE;

                FETCH c_batch
                BULK COLLECT INTO l_batch_tbl;
            CLOSE c_batch;


            OPEN c_item_data(c.source_system_id);
                l_item_tab_tbl.DELETE;

                FETCH c_item_data
                BULK COLLECT INTO l_item_tab_tbl;

            l_record_no                     := 1;
            ln_batch_indx                   := 1;
            l_partition_size                := l_item_tab_tbl.COUNT;
            l_partition_size                := CEIL( l_partition_size / gn_no_of_threads ) + 1;
            l_new_set_process_id            := -1;

            Log_Msg('l_partition_size       : '||l_partition_size);

            FOR ln_indx IN l_item_tab_tbl.FIRST .. l_item_tab_tbl.LAST
            LOOP

                IF ( MOD (l_record_no, l_partition_size) = 0 ) OR  (l_record_no = 1)
                THEN
                    -- Assigning the New Batch
                    IF l_batch_tbl.EXISTS(ln_batch_indx)
                    THEN
                        -- Assigning an existing Batch
                        l_new_set_process_id        := l_batch_tbl(ln_batch_indx).batch_id;
                        ln_batch_indx               := ln_batch_indx + 1;
                    ELSE
                        -- Creating and Assigning a Batch
                        l_new_set_process_id        := create_import_batch( c.source_system_id );
                    END IF;

                    Log_Msg('l_record_no            : '||l_record_no);
                    Log_Msg('l_new_set_process_id   : '||l_new_set_process_id);
                END IF;

                IF l_new_set_process_id <> -1
                THEN
                    l_item_tab_tbl(ln_indx).set_process_id  := l_new_set_process_id;
                END IF;

                l_record_no                             := l_record_no + 1;

            END LOOP; -- FOR ln_indx IN l_item_tab_tbl.FIRST .. l_item_tab_tbl.LAST

            -----------------------------------------------------------
            -- Updating the Item Staging Table
            -----------------------------------------------------------
            BEGIN
                 FORALL i IN INDICES OF l_item_tab_tbl SAVE EXCEPTIONS
                 UPDATE xxpso_ego_items_stg
                    SET set_process_id              = l_item_tab_tbl(i).set_process_id
                       ,last_update_date            = SYSDATE
                       ,last_updated_by             = gcn_user_id
                       ,conc_request_id             = gcn_request_id
                  WHERE record_id                   = l_item_tab_tbl(i).record_id;

                COMMIT;
            EXCEPTION
                WHEN OTHERS THEN
                     Log_Msg('Error while updating staging table.');
                     gn_retcode  := gcn_retcode_warning;
            END;

            CLOSE c_item_data;

        END LOOP;       -- FOR c IN c_ss_data

        -----------------------------------------------------------
        -- Updating the UDA Staging Table
        -----------------------------------------------------------
        BEGIN
	        UPDATE xxpso_ego_item_attr_stg x
               SET set_process_id       = (SELECT MAX(y.set_process_id)
                                             FROM xxpso_ego_items_stg y
                                            WHERE y.transaction_id  = x.transaction_id
                                          )
	         WHERE batch_id             = gn_batch_id
	           AND status_code          IN (gc_valid_succ_flag, gc_import_error_flag)
               AND transaction_id       IS NOT NULL;

            COMMIT;
        EXCEPTION
            WHEN OTHERS THEN
                 Log_Msg('Error while updating staging table.');
                 gn_retcode  := gcn_retcode_warning;
        END;


        Log_Msg('***  SETTING THE DATA SET ID FOR MULTI THREADING - COMPLETED ***');
        Log_Msg('****************************************************************');

    EXCEPTION
        WHEN OTHERS THEN
            l_error_message := 'Unexpected Error in partition_data procedure. SQLERRM : ' || SQLERRM;
            Log_Msg(l_error_message);

            gn_retcode  := gcn_retcode_warning;
    END partition_data;




   /* ************************************************************************************
   * Procedure: transfer_item_data
   *
   * Synopsis: This procedure is to transfer valid Item staging records to Item Interface table
   *
   * PARAMETERS:
   *   OUT:
   *   IN:
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            28-JAN-2016
   ************************************************************************************* */
    PROCEDURE transfer_item_data
    AS
	    --local variable declaration
	    l_error_flag                    VARCHAR2(1)         :=gc_trans_succ_flag;
		l_error_message                 VARCHAR2(4000);

	    --Cursor declaration
        CURSOR c_item_data
        IS
            SELECT *
	          FROM xxpso_ego_items_stg
	         WHERE batch_id         = gn_batch_id
	           AND status_code      IN (gc_valid_succ_flag, gc_import_error_flag)
               AND transaction_id   IS NOT NULL;

        TYPE l_item_tab_type IS TABLE OF            c_item_data%ROWTYPE INDEX BY BINARY_INTEGER;
        l_item_tab_tbl                              l_item_tab_type;

    BEGIN

        Log_Msg('****************************************************************');
        Log_Msg('****        ITEM INTERFACE TABLE INSERTS - STARTED         *****');

        OPEN c_item_data;
        LOOP
            l_item_tab_tbl.DELETE;

            FETCH c_item_data
            BULK COLLECT INTO l_item_tab_tbl LIMIT gcn_item_bulk_limit;

            IF (l_item_tab_tbl.COUNT > 0)
            THEN
                -----------------------------------------------------------
                -- Inserting Into Interface Table
                -----------------------------------------------------------
                BEGIN
                    FORALL i IN INDICES OF l_item_tab_tbl SAVE EXCEPTIONS
                        INSERT INTO mtl_system_items_interface
                        (
                         transaction_id
                        ,inventory_item_id
                        ,organization_id
                        ,organization_code
                        ,description
                        ,long_description
                        ,inventory_item_status_code
                        ,primary_unit_of_measure
                        ,item_type
                        ,item_catalog_group_id
                        ,template_id
                        ,set_process_id
                        ,source_system_id
                        ,source_system_reference
                        ,source_system_reference_desc
                        ,lifecycle_id
                        ,current_phase_id
                        ,dimension_uom_code
                        ,unit_length
                        ,unit_width
                        ,unit_height
                        ,weight_uom_code
                        ,unit_weight
                        ,volume_uom_code
                        ,unit_volume
                        ,created_by
                        ,creation_date
                        ,process_flag
                        ,transaction_type
                        ,confirm_status
                        ,eng_item_flag
                        )
                        VALUES
                        (
                         l_item_tab_tbl(i).transaction_id
                        ,l_item_tab_tbl(i).inventory_item_id
                        ,l_item_tab_tbl(i).organization_id
                        ,l_item_tab_tbl(i).organization_code
                        ,l_item_tab_tbl(i).description
                        ,l_item_tab_tbl(i).long_description
                        ,l_item_tab_tbl(i).inventory_item_status_code
                        ,l_item_tab_tbl(i).primary_unit_of_measure
                        ,l_item_tab_tbl(i).item_type
                        ,l_item_tab_tbl(i).item_catalog_group_id
                        ,l_item_tab_tbl(i).item_template_id
                        ,l_item_tab_tbl(i).set_process_id
                        ,l_item_tab_tbl(i).source_system_id
                        ,l_item_tab_tbl(i).source_item_number
                        ,l_item_tab_tbl(i).description
                        ,l_item_tab_tbl(i).lifecycle_id
                        ,l_item_tab_tbl(i).current_phase_id
                        ,l_item_tab_tbl(i).dimension_uom
                        ,l_item_tab_tbl(i).length
                        ,l_item_tab_tbl(i).width
                        ,l_item_tab_tbl(i).height
                        ,l_item_tab_tbl(i).weight_uom
                        ,l_item_tab_tbl(i).weight
                        ,l_item_tab_tbl(i).volume_uom
                        ,l_item_tab_tbl(i).volume
                        ,l_item_tab_tbl(i).created_by
                        ,SYSDATE
                        ,1
                        ,'SYNC'
                        ,'CN'
                        ,'N'
                        );

                    COMMIT;
                EXCEPTION
                    WHEN OTHERS THEN
                        Debug_Msg('Error while inserting into Interface Table - '|| SQLERRM);
                        gn_retcode  := gcn_retcode_warning;
                END;

                -----------------------------------------------------------
                -- Updating the Staging Table
                -----------------------------------------------------------
                BEGIN
                     FORALL i IN INDICES OF l_item_tab_tbl SAVE EXCEPTIONS
                     UPDATE xxpso_ego_items_stg
                        SET status_code                 = gc_trans_succ_flag
                           ,last_update_date            = SYSDATE
                           ,last_updated_by             = gcn_user_id
                           ,conc_request_id             = gcn_request_id
                      WHERE record_id                   = l_item_tab_tbl(i).record_id;

                    COMMIT;
                EXCEPTION
                    WHEN OTHERS THEN
                         Debug_Msg('Error while updating staging table.');
                         gn_retcode  := gcn_retcode_warning;
                END;

            END IF;

            EXIT WHEN c_item_data%NOTFOUND;

        END LOOP;

        CLOSE c_item_data;

        Log_Msg('****        ITEM INTERFACE TABLE INSERTS - COMPLETED       *****');
        Log_Msg('****************************************************************');

    EXCEPTION
    WHEN OTHERS THEN
            l_error_message := 'Unexpected Error in transfer_item_data procedure. SQLERRM : ' || SQLERRM;
            Log_Msg(l_error_message);

            gn_retcode  := gcn_retcode_warning;
    END transfer_item_data;




   /* ************************************************************************************
   * Procedure: transfer_uda_data
   *
   * Synopsis: This procedure is to transfer valid UDA staging records to UDA Interface table
   *
   * PARAMETERS:
   *   OUT:
   *   IN:
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            28-JAN-2016
   ************************************************************************************* */
    PROCEDURE transfer_uda_data
    AS
	    --local variable declaration
	    l_error_flag                    VARCHAR2(1)         :=gc_trans_succ_flag;
		l_error_message                 VARCHAR2(4000);

	    --Cursor for those UDA records which has corresponding record in Item STG also
        CURSOR c_uda_data
        IS
            SELECT *
	          FROM xxpso_ego_item_attr_stg
	         WHERE batch_id             = gn_batch_id
	           AND status_code          IN (gc_valid_succ_flag, gc_import_error_flag)
               AND transaction_id       IS NOT NULL;

        TYPE l_uda_tab_type IS TABLE OF             c_uda_data%ROWTYPE INDEX BY BINARY_INTEGER;
        l_uda_tab_tbl                               l_uda_tab_type;

    BEGIN

        Log_Msg('****************************************************************');
        Log_Msg('****        UDA INTERFACE TABLE INSERTS - STARTED          *****');

        OPEN c_uda_data;
        LOOP
            l_uda_tab_tbl.DELETE;

            FETCH c_uda_data
            BULK COLLECT INTO l_uda_tab_tbl LIMIT gcn_uda_bulk_limit;

            IF (l_uda_tab_tbl.COUNT > 0)
            THEN
                -----------------------------------------------------------
                -- Inserting Into Interface Table
                -----------------------------------------------------------
                BEGIN
                    FORALL i IN INDICES OF l_uda_tab_tbl SAVE EXCEPTIONS
                        INSERT INTO ego_itm_usr_attr_intrfc
                        (
                         transaction_id
                        ,organization_code
                        ,organization_id
                        ,inventory_item_id
                        ,data_set_id
                        ,attr_group_int_name
                        ,attr_int_name
                        ,attr_value_str
                        ,attr_value_num
                        ,attr_value_date
                        ,row_identifier
                        ,source_system_id
                        ,source_system_reference
                        ,item_catalog_group_id
                        ,attr_group_type
                        ,data_level_id
                        ,created_by
                        ,creation_date
                        ,process_status
                        ,transaction_type
                        )
                        VALUES
                        (
                         l_uda_tab_tbl(i).transaction_id
                        ,l_uda_tab_tbl(i).organization_code
                        ,l_uda_tab_tbl(i).organization_id
                        ,l_uda_tab_tbl(i).inventory_item_id
                        ,l_uda_tab_tbl(i).set_process_id
                        ,l_uda_tab_tbl(i).attr_group_name
                        ,l_uda_tab_tbl(i).attr_name
                        ,l_uda_tab_tbl(i).attr_char_value
                        ,l_uda_tab_tbl(i).attr_num_value
                        ,l_uda_tab_tbl(i).attr_date_value
                        ,l_uda_tab_tbl(i).row_identifier
                        ,l_uda_tab_tbl(i).source_system_id
                        ,l_uda_tab_tbl(i).source_item_number
                        ,l_uda_tab_tbl(i).item_catalog_group_id
                        ,l_uda_tab_tbl(i).attr_group_type
                        ,l_uda_tab_tbl(i).data_level_id
                        ,l_uda_tab_tbl(i).created_by
                        ,SYSDATE
                        ,1
                        ,'SYNC'
                        );
                    COMMIT;
                EXCEPTION
                    WHEN OTHERS THEN
                        Debug_Msg('Error while inserting into Interface Table - '|| SQLERRM);
                        gn_retcode  := gcn_retcode_warning;
                END;

                -----------------------------------------------------------
                -- Updating the Staging Table
                -----------------------------------------------------------
                BEGIN
                     FORALL i IN INDICES OF l_uda_tab_tbl SAVE EXCEPTIONS
                     UPDATE xxpso_ego_item_attr_stg
                        SET status_code                 = gc_trans_succ_flag
                           ,last_update_date            = SYSDATE
                           ,last_updated_by             = gcn_user_id
                           ,conc_request_id             = gcn_request_id
                      WHERE record_id                   = l_uda_tab_tbl(i).record_id;

                    COMMIT;
                EXCEPTION
                    WHEN OTHERS THEN
                         Debug_Msg('Error while updating staging table.');
                         gn_retcode  := gcn_retcode_warning;
                END;

            END IF;

            EXIT WHEN c_uda_data%NOTFOUND;

        END LOOP;

        CLOSE c_uda_data;

        Log_Msg('****       UDA INTERFACE TABLE INSERTS - COMPLETED          *****');
        Log_Msg('****************************************************************');

    EXCEPTION
    WHEN OTHERS THEN
            l_error_message := 'Unexpected Error in transfer_uda_data procedure. SQLERRM : ' || SQLERRM;
            Log_Msg(l_error_message);

            gn_retcode  := gcn_retcode_warning;
    END transfer_uda_data;




PROCEDURE import_data
    AS
	    --local variable declaration
	    l_error_flag                    VARCHAR2(1)         :=gc_import_succ_flag;
		l_error_message                 VARCHAR2(4000);
        l_request_id                    NUMBER;
        ln_idx                          NUMBER              := 1;
        lb_wait                         BOOLEAN;
        lv_phase                        VARCHAR2 (2000)     := '';
        lv_status                       VARCHAR2 (2000)     := '';
        lv_dev_phase                    VARCHAR2 (2000)     := '';
        lv_dev_status                   VARCHAR2 (2000)     := '';
        lv_message                      VARCHAR2 (2000)     := '';

        -- Type and Record Variables
        TYPE l_request_ids_rec_type     IS RECORD  (request_id  NUMBER);
        TYPE l_request_ids_tbl_type     IS TABLE OF l_request_ids_rec_type  INDEX BY BINARY_INTEGER;
        l_request_ids_tbl               l_request_ids_tbl_type;
        l_xref_tbl                      mtl_cross_references_pub.xref_tbl_type;
        l_message_list                  error_handler.error_tbl_type;

	    --Cursor declaration
        CURSOR c_item_intf_data
        IS
             SELECT y.transaction_id, y.process_flag, y.inventory_item_id, x.batch_id, x.source_system, x.source_item_number
               FROM xxpso_ego_items_stg x, mtl_system_items_interface y
              WHERE x.batch_id           = gn_batch_id
                AND x.transaction_id     = y.transaction_id
                AND x.status_code        = gc_trans_succ_flag;

        CURSOR c_item_intf_data2
        IS
             SELECT DISTINCT x.transaction_id, x.batch_id, x.source_system, x.source_item_number
               FROM xxpso_ego_items_stg x
              WHERE x.batch_id           = gn_batch_id
                AND x.status_code        = gc_import_error_flag;

        CURSOR c_uda_intf_data
        IS
             SELECT y.transaction_id, y.process_status, x.batch_id, x.source_system, x.source_item_number, x.attr_group_name, x.attr_name
               FROM xxpso_ego_item_attr_stg x, ego_itm_usr_attr_intrfc y
              WHERE x.batch_id           = gn_batch_id
                AND x.transaction_id     = y.transaction_id
                AND x.attr_group_name    = y.attr_group_int_name
                AND x.attr_name          = y.attr_int_name
                AND x.status_code        = gc_trans_succ_flag;

        CURSOR c_uda_intf_data2
        IS
             SELECT DISTINCT x.transaction_id, x.batch_id, x.source_system, x.source_item_number
               FROM xxpso_ego_item_attr_stg x
              WHERE x.batch_id           = gn_batch_id
                AND x.status_code        = gc_import_error_flag;

        CURSOR c_item_err_data (p_transaction_id NUMBER)
        IS
             SELECT DISTINCT mie.error_message
               FROM mtl_interface_errors mie
              WHERE mie.transaction_id  = p_transaction_id
                AND mie.error_message   NOT LIKE '%is not a valid Inventory Item ID in the passed-in Organization%'
                AND mie.table_name      <> 'EGO_ITM_USR_ATTR_INTRFC';

        CURSOR c_uda_err_data (p_transaction_id NUMBER)
        IS
             SELECT DISTINCT mie.error_message
               FROM mtl_interface_errors mie
              WHERE mie.transaction_id  = p_transaction_id
                AND mie.error_message   NOT LIKE '%is not a valid Inventory Item ID in the passed-in Organization%'
                AND mie.table_name      = 'EGO_ITM_USR_ATTR_INTRFC';

    BEGIN
        Log_Msg('****************************************************************');
        Log_Msg('****    LAUNCH OF IMPORT CATLOG ITEM PROGRAM - STARTED     *****');

        BEGIN
            FOR l_inf_cur in (SELECT DISTINCT set_process_id, organization_id FROM inv.mtl_system_items_interface WHERE process_flag = 1 ORDER BY set_process_id )
            LOOP
                l_request_id := fnd_request.submit_request(
                                 application    => 'EGO'                             --Application Short Name
                                ,program        => 'EGOICI'                          --Program Short Name
                                ,description    => ''                                --Program description
                                ,argument1      => l_inf_cur.organization_id         --Current Organization Code
                                ,argument2      => '1'                               --All Organizations
                                ,argument3      => '1'                               --Validate Items
                                ,argument4      => '1'                               --Process Items
                                ,argument5      => '2'                               --Delete Processed Records
                                ,argument6      => l_inf_cur.set_process_id          --Process Set
                                ,argument7      => '3'                               --CREATE (1) or UPDATE (2) or SYNC (3)
                                  );
            COMMIT;

                l_request_ids_tbl(ln_idx).request_id    := l_request_id;
                ln_idx                                  := ln_idx + 1;

                COMMIT;
            END LOOP;

            IF l_request_ids_tbl.COUNT > 0
            THEN

                FOR ln_idx IN l_request_ids_tbl.FIRST .. l_request_ids_tbl.LAST
                LOOP
                    LOOP
                        lb_wait := fnd_concurrent.get_request_status
                                                        (l_request_ids_tbl(ln_idx).request_id,
                                                         'EGO',
                                                         'EGOICI',
                                                         lv_phase,
                                                         lv_status,
                                                         lv_dev_phase,
                                                         lv_dev_status,
                                                         lv_message
                                                        );

                        EXIT WHEN lv_dev_phase = 'COMPLETE';
                        DBMS_LOCK.SLEEP(10);

                    END LOOP;
                END LOOP;
            END IF;

        EXCEPTION
        WHEN OTHERS THEN
            Debug_Msg('Error in launching Import Catalog Items program - '|| SQLERRM);
            gn_retcode  := gcn_retcode_error;
        END;

        Log_Msg('****    LAUNCH OF IMPORT CATLOG ITEM PROGRAM - COMPLETED   *****');
        Log_Msg('****************************************************************');


        Log_Msg('****************************************************************');
        Log_Msg('****    RECONCILATION OF ITEM INTERFACE TABLE - STARTED    *****');


       /*  FOR cur_intf_rec IN c_item_intf_data
        LOOP
            l_error_flag        := gc_import_succ_flag;
            l_error_message     := NULL;

            Debug_Msg('Verifying Interface Status of record for Source System - '|| cur_intf_rec.source_system || ' Source Item Number ' || cur_intf_rec.source_item_number);

            IF cur_intf_rec.process_flag = 7
            THEN
                l_error_flag        := gc_import_succ_flag;
                Debug_Msg('Record Imported Successfully');

            ELSE
                l_error_flag        := gc_import_error_flag;
                gn_retcode          := gcn_retcode_warning;
                --l_error_message     := get_interface_error('MTL_SYSTEM_ITEMS_INTERFACE',cur_intf_rec.transaction_id);

                FOR cur_err_rec IN c_item_err_data(cur_intf_rec.transaction_id)
                LOOP
                    CAPTURE_ERROR(  gc_item_ricew_id
                                    ,cur_intf_rec.batch_id
                                    ,cur_intf_rec.source_system
                                    ,cur_intf_rec.source_item_number
                                    ,NULL
                                    ,'TRANSACTION_ID'
                                    ,cur_intf_rec.transaction_id
                                    ,SUBSTR( cur_err_rec.error_message , 1, 3999)
                                    ,'Contact Technical Team'
                                    ,NULL
                                    );
                    l_error_message     := SUBSTR( l_error_message||cur_err_rec.error_message||'; ', 1, 3999);
                END LOOP;

                Debug_Msg('Record Failed to Import. Error - ' || l_error_message);

            END IF;

         UPDATE xxpso_ego_items_stg x
            SET status_code             = l_error_flag,
                error_message           = l_error_message,
                inventory_item_id       = cur_intf_rec.inventory_item_id,
                last_update_date        = SYSDATE,
                last_updated_by         = gcn_user_id,
                conc_request_id         = gcn_request_id
          WHERE batch_id                = cur_intf_rec.batch_id
            AND transaction_id          = cur_intf_rec.transaction_id
            AND source_system           = cur_intf_rec.source_system
            AND source_item_number      = cur_intf_rec.source_item_number;

        END LOOP;
        COMMIT;      */

         UPDATE xxpso_ego_items_stg stg
            SET status_code        = DECODE( (SELECT COUNT(1)
                                             FROM mtl_system_items_interface itf
                                            WHERE itf.transaction_id  = stg.transaction_id
                                              AND process_flag        = 7
                                           ), 1, gc_import_succ_flag, gc_import_error_flag ),
                inventory_item_id  = (SELECT MAX(itf.inventory_item_id)
                                     FROM mtl_system_items_interface itf
                                    WHERE itf.transaction_id  = stg.transaction_id
                                      AND itf.process_flag        = 7
                                   ),
                last_update_date        = SYSDATE,
                last_updated_by         = gcn_user_id,
                conc_request_id         = gcn_request_id
          WHERE batch_id                = gn_batch_id
            AND status_code             = gc_trans_succ_flag;
        COMMIT;

        FOR cur_intf_rec IN c_item_intf_data2
        LOOP
            --l_error_flag        := gc_import_succ_flag;
            l_error_message     := NULL;

            Debug_Msg('Getting Item error messages for transaction_id - '|| cur_intf_rec.transaction_id );

            FOR cur_err_rec IN c_item_err_data(cur_intf_rec.transaction_id)
            LOOP
                 CAPTURE_ERROR(  gc_item_ricew_id
                                ,cur_intf_rec.batch_id
                                ,cur_intf_rec.source_system
                                ,cur_intf_rec.source_item_number
                                ,NULL
                                ,'TRANSACTION_ID'
                                ,cur_intf_rec.transaction_id
                                ,SUBSTR( cur_err_rec.error_message , 1, 3999)
                                ,'Contact Technical Team'
                                ,NULL
                                );
                l_error_message     := SUBSTR( l_error_message||cur_err_rec.error_message||'; ', 1, 3999);
            END LOOP;

        UPDATE xxpso_ego_items_stg x
            SET error_message           = l_error_message
          WHERE batch_id                = cur_intf_rec.batch_id
            AND transaction_id          = cur_intf_rec.transaction_id
            AND source_system           = cur_intf_rec.source_system
            AND source_item_number      = cur_intf_rec.source_item_number
            AND status_code             = gc_import_error_flag;

        END LOOP;
        COMMIT;


        Log_Msg('****    RECONCILATION OF ITEM INTERFACE TABLE - COMPLETED  *****');
        Log_Msg('****************************************************************');

        Log_Msg('****************************************************************');
        Log_Msg('****    RECONCILATION OF UDA INTERFACE TABLE - STARTED     *****');


      /*   FOR cur_intf_rec IN c_uda_intf_data
        LOOP
            l_error_flag        := gc_import_succ_flag;
            l_error_message     := NULL;

            Debug_Msg('Verifying Interface Status of record for Source System - '|| cur_intf_rec.source_system || ' Source Item Number ' || cur_intf_rec.source_item_number);

            IF cur_intf_rec.process_status = 4
            THEN
                l_error_flag        := gc_import_succ_flag;
                Debug_Msg('Record Imported Successfully');

            ELSE
                l_error_flag        := gc_import_error_flag;
                gn_retcode          := gcn_retcode_warning;
                --l_error_message     := get_interface_error('EGO_ITM_USR_ATTR_INTRFC',cur_intf_rec.transaction_id);
                 Debug_Msg('Record Failed to Import. Error - ');

            END IF;

         UPDATE xxpso_ego_item_attr_stg x
            SET status_code             = l_error_flag,
                --error_message           = l_error_message,
                last_update_date        = SYSDATE,
                last_updated_by         = gcn_user_id,
                conc_request_id         = gcn_request_id
          WHERE batch_id                = cur_intf_rec.batch_id
            AND transaction_id          = cur_intf_rec.transaction_id
            AND attr_group_name         = cur_intf_rec.attr_group_name
            AND attr_name               = cur_intf_rec.attr_name;

        END LOOP;
        COMMIT;   */



         UPDATE xxpso_ego_item_attr_stg stg
            SET status_code             = DECODE( (SELECT COUNT(1)
                                                    FROM ego_itm_usr_attr_intrfc itf
                                                   WHERE itf.transaction_id         = stg.transaction_id
                                                     AND itf.attr_group_int_name    = stg.attr_group_name
                                                     AND itf.attr_int_name          = stg.attr_name
                                                     AND itf.row_identifier         = stg.row_identifier
                                                     AND itf.process_status         = 4
                                                  ), 1, gc_import_succ_flag, gc_import_error_flag  ),
                last_update_date        = SYSDATE,
                last_updated_by         = gcn_user_id,
                conc_request_id         = gcn_request_id
          WHERE batch_id                = gn_batch_id
            AND status_code             = gc_trans_succ_flag;
        COMMIT;

        FOR cur_intf_rec IN c_uda_intf_data2
        LOOP
            --l_error_flag        := gc_import_succ_flag;
            l_error_message     := NULL;

            Debug_Msg('Getting UDA error messages for transaction_id - '|| cur_intf_rec.transaction_id );

            FOR cur_err_rec IN c_uda_err_data(cur_intf_rec.transaction_id)
            LOOP
                 CAPTURE_ERROR(  gc_uda_ricew_id
                                ,cur_intf_rec.batch_id
                                ,cur_intf_rec.source_system
                                ,cur_intf_rec.source_item_number
                                ,NULL
                                ,'TRANSACTION_ID'
                                ,cur_intf_rec.transaction_id
                                ,SUBSTR( cur_err_rec.error_message , 1, 3999)
                                ,'Contact Technical Team'
                                ,NULL
                                );
                l_error_message     := SUBSTR( l_error_message||cur_err_rec.error_message||'; ', 1, 3999);
            END LOOP;

        UPDATE xxpso_ego_item_attr_stg x
            SET error_message           = l_error_message
          WHERE batch_id                = cur_intf_rec.batch_id
            AND transaction_id          = cur_intf_rec.transaction_id
            AND source_system           = cur_intf_rec.source_system
            AND source_item_number      = cur_intf_rec.source_item_number
            AND status_code             = gc_import_error_flag;

        END LOOP;
        COMMIT;

        Log_Msg('****    RECONCILATION OF UDA INTERFACE TABLE - COMPLETED   *****');
        Log_Msg('****************************************************************');

    EXCEPTION
    WHEN OTHERS THEN
            l_error_message := 'Unexpected Error in import_data procedure. SQLERRM : ' || SQLERRM;
            Log_Msg(l_error_message);

            gn_retcode  := gcn_retcode_warning;
    END import_data;



   /* ************************************************************************************
   * Procedure: main
   *
   * Synopsis: This procedure will be called from Concurrent Program "XXPSO EGO Item Interface Program".
   *           It will internally call below procedures based on the parameter values :-
   *           1. update_batch_id     2. gather_table_stats    3. validate_item_data    4. validate_uda_data
   *           4. purge_if_tables     5. archive_stg_tables    6. partition_data        7. transfer_item_data
   *           8. transfer_uda_data   9. import_data          10. XXPSO_CMN_CNV_PKG.print_error_details
   *
   * PARAMETERS:
   *   OUT:
   *        p_errbuf                VARCHAR2        -- Buffer variable for error message
   *        p_retcode               NUMBER          -- Return code variable to indicate program completion status
   *   IN:
   *        p_mode                  VARCHAR2        -- Its value may be 'V' or 'P'
   *        p_record_status         VARCHAR2        -- Its value may be 'All' or 'New' or 'Failed'
   *        p_batch_id              NUMBER          -- It is the batch identifier
   *        p_batch_size            NUMBER          -- It is the batch size
   *        p_no_of_threads         NUMBER          -- This parameters decide number of Import Program to fire
   *        p_purge_if_tables       VARCHAR2        -- Its value may be 'All' or 'Processed' or 'Unprocessed'
   *        p_archive_stg_tables    VARCHAR2        -- Its value may be 'All' or 'Processed' or 'Unprocessed'
   *        p_debug_flag            VARCHAR2        -- Its value may be 'Y' or 'N'

   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            22-Sep-2014
   * Narendra Mishra    1.1 Added Batch Size Parameter                 17-MAR-2016
   * Narendra Mishra    1.2 Added p_no_of_threads, p_purge_if_tables   22-MAR-2016
   *                        and p_archive_stg_tables  Parameter
   * Narendra Mishra    1.3 Added p_record_status Parameter            29-MAR-2016
   ************************************************************************************* */
    PROCEDURE main (  p_errbuf              OUT   NOCOPY   VARCHAR2
                     ,p_retcode             OUT   NOCOPY   NUMBER
                     ,p_mode                IN             VARCHAR2
                     ,p_record_status       IN             VARCHAR2
                     ,p_batch_id            IN             NUMBER
                     ,p_batch_size          IN             NUMBER
                     ,p_no_of_threads       IN             NUMBER
                     ,p_purge_if_tables     IN             VARCHAR2
                     ,p_archive_stg_tables  IN             VARCHAR2
                     ,p_debug_flag          IN             VARCHAR2
                    )
    IS
		--local variable declaration

        l_request_id        NUMBER              := NULL;
        l_mode_desc         VARCHAR2 (50)       := NULL;
        l_error_message     VARCHAR2 (4000)     := NULL;
        lv_prog_name        VARCHAR2 (200)      := NULL;
        lv_str              VARCHAR2 (1000)     := '************************';
        lv_str2             VARCHAR2 (1000)     := '---------';

        CURSOR c_cnv_summ
        IS
            SELECT DISTINCT A.ricew_id, A.ricew_desc, b.total_records, b.total_not_process_records , b.total_valid_records, b.total_fail_records, b.total_int_records, b.total_process_records
              FROM xxpso_cmn_cnv_metadata A, xxpso_cmn_cnv_sum b
             WHERE A.ricew_id     = b.ricew_id
               AND A.ricew_group  = gc_ricew_group
          ORDER BY A.ricew_id;

    BEGIN
        BEGIN
            SELECT user_concurrent_program_name
              INTO lv_prog_name
              FROM fnd_concurrent_programs_tl
             WHERE concurrent_program_id = FND_GLOBAL.conc_program_id
               AND language = 'US';

             l_request_id   := fnd_global.conc_request_id;
        EXCEPTION
            WHEN OTHERS THEN
                lv_prog_name := NULL;
        END;

        -- Printing Log Messages
        Log_Msg( lv_str || ' ' || lv_prog_name || ' ' || lv_str );
        Log_Msg( '  ');
        Log_Msg( 'Request ID            : ' || l_request_id);
        Log_Msg( 'Program Run Date      : ' || TO_CHAR (SYSDATE, 'MM-DD-RRRR'));
        Log_Msg( 'Parameters ----------------------------------------------- ' );
        Log_Msg( 'Process Mode          : ' || p_mode);
        Log_Msg( 'Record Status         : ' || p_record_status);
        Log_Msg( 'Batch ID              : ' || p_batch_id);
        Log_Msg( 'Batch Size            : ' || p_batch_size);
        Log_Msg( 'No of Thread          : ' || p_no_of_threads);
        Log_Msg( 'Purge IF Tables       : ' || p_purge_if_tables);
        Log_Msg( 'Archive STG Tables    : ' || p_archive_stg_tables);
        Log_Msg( 'Debug Flag            : ' || p_debug_flag);
        Log_Msg( '  ');

        gc_record_status    := NVL(p_record_status,'All');
        gn_batch_size       := NVL(p_batch_size,0);
        gc_debug_flag       := NVL(p_debug_flag,'N');
        gn_no_of_threads    := NVL(p_no_of_threads,0);

        
        -- Added on 25-APR-2016
        IF gc_enable_de_duplication_flag    = gc_yes
        THEN
            --calling procedure to perform item de-duplication
            perform_de_duplication;
        END IF;
        
        
        IF p_archive_stg_tables             = gc_yes
        THEN
            --calling procedure to archive staging table records
            Debug_Msg( '+++ Calling IDENTIFY_ARCHIVE_RECORDS procedure +++' );
            identify_archive_records;
            
            Debug_Msg( '+++ Calling ARCHIVE_STG_TABLES procedure +++' );
            archive_stg_tables;         
        END IF;           
        
        IF p_purge_if_tables IS NOT NULL
        THEN
            --calling procedure to purge Interface Tables records
            Debug_Msg( '+++ Calling PURGE_IF_TABLES procedure +++' );
            purge_if_tables (p_purge_if_tables);
        END IF;

     
        IF p_batch_id IS NULL
        THEN
            gn_batch_id  := TO_CHAR(SYSDATE, 'YYYYMMDDHH24MI');

            Log_Msg('Setting Batch Id as - '|| gn_batch_id);
            update_batch_id;
        ELSE
            gn_batch_id         := p_batch_id;
        END IF;


        -- CALL IN BOTH VALIDATE AND IMPORT MODE
        IF p_mode   IN ('V', 'P')
        THEN
            l_mode_desc := 'VALIDATE';

            --calling procedure to gather stats
            gather_table_stats;

            --calling procedure to validate records in Item staging table
            Debug_Msg( '+++ Calling VALIDATE_ITEM_DATA procedure +++' );
            validate_item_data;

            --calling procedure to validate records in UDA staging table
            Debug_Msg( '+++ Calling VALIDATE_UDA_DATA procedure +++' );
            validate_uda_data;

        END IF;


        -- CALL IN ONLY IN IMPORT MODE
        IF p_mode   = 'P'
        THEN
            l_mode_desc := 'IMPORT';

            IF gn_no_of_threads > 0
            THEN
                --calling procedure to partition records based on Import Batch ID
                Debug_Msg( '+++ Calling PARTITION_DATA procedure +++' );
                partition_data;
            END IF;

            --calling procedure to transfer valid Item staging records into Interface table
            Debug_Msg( '+++ Calling TRANSFER_ITEM_DATA procedure +++' );
            transfer_item_data;

            --calling procedure to transfer valid UDA staging records into Interface table
            Debug_Msg( '+++ Calling TRANSFER_UDA_DATA procedure +++' );
            transfer_uda_data;

            --calling procedure to gather stats
            gather_table_stats;

            --call procedure to import data
            Debug_Msg( '+++ Calling IMPORT_DATA procedure +++' );
            import_data;

        END IF;


		--calling common utility procedure to generate excel report
        Debug_Msg( '+++ Calling XXPSO_CMN_CNV_PKG.PRINT_ERROR_DETAILS procedure +++' );
        XXPSO_CMN_CNV_PKG.print_error_details(  p_request_id     => l_request_id,
                                                p_rice_group     => gc_ricew_group,
                                                p_operation      => l_mode_desc,
                                                p_primary_hdr    => 'Batch ID',
                                                p_secondary_hdr  => 'Source System',
                                                p_tri_hdr        => 'Source Item Number'
                                                );


        -- Generating a report in Output File
        Out_Msg( lv_str || ' ' || lv_prog_name || ' ' || lv_str );
        Out_Msg( '  ');
        Out_Msg( 'Request ID            : ' || l_request_id);
        Out_Msg( 'Program Run Date      : ' || TO_CHAR (SYSDATE, 'MM-DD-RRRR'));
        Out_Msg( 'Parameters ----------------------------------------------- ' );
        Out_Msg( 'Process Mode          : ' || p_mode);
        Out_Msg( 'Record Status         : ' || p_record_status);
        Out_Msg( 'Batch ID              : ' || p_batch_id);
        Out_Msg( 'Batch Size            : ' || p_batch_size);
        Out_Msg( 'No of Thread          : ' || p_no_of_threads);
        Out_Msg( 'Purge IF Tables       : ' || p_purge_if_tables);
        Out_Msg( 'Archive STG Tables    : ' || p_archive_stg_tables);
        Out_Msg( 'Debug Flag            : ' || p_debug_flag);
        Out_Msg( '  ');

        FOR c_rec IN c_cnv_summ
        LOOP
                Out_Msg( lv_str2 || ' Reconciliation summary of ' || c_rec.ricew_desc || ' ' || lv_str2 );
                Out_Msg( 'Total number of records                           : ' || c_rec.total_records );
                Out_Msg( 'Total number of unprocessed records               : ' || c_rec.total_not_process_records );
                Out_Msg( 'Total number of sucessfully validated records     : ' || c_rec.total_valid_records );
                Out_Msg( 'Total number of records failed API/Interface      : ' || c_rec.total_fail_records );
                Out_Msg( 'Total number of records interface                 : ' || c_rec.total_int_records );
                Out_Msg( 'Total number of records successfully imported     : ' || c_rec.total_process_records );
                Out_Msg( ' ' );
         END LOOP;

	    p_retcode := gn_retcode;

    EXCEPTION
    WHEN OTHERS THEN
            l_error_message := 'Unexpected Error in main procedure. SQLERRM : ' || SQLERRM;
            Log_Msg(l_error_message);

            p_retcode  := gcn_retcode_warning;
    END;


END XXPSO_EGO_PROCESS_ITEMS_PKG;
/

SHOW ERRORS
EXIT SUCCESS 

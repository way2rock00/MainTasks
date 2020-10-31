WHENEVER SQLERROR EXIT FAILURE

create or replace PACKAGE BODY      XXPSO_PDH_BATCH_OUTBOUND_PKG
AS
/*=============================================================================================+
|                               Copyright (c) 2016 Pearson                                     |
|                                 All rights reserved.                                         |
+==============================================================================================+
|
| Header            : $1.0
| File              : XXPSO_PDH_BATCH_OUTBOUND_PKG.pkb 
| Package Name      : XXPSO_PDH_BATCH_OUTBOUND_PKG
| Developed By      : Narendra Mishra
| Description       : Package Body for PDH Batch Outbound Integration through ETL 
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
|  Narendra Mishra      | 23-Jun-2016   | 1.0      | Initial Revision
|  Narendra Mishra      | 22-Aug-2016   | 1.1      | Changed the code as per new mapping document
|  Akshay Nayak         | 24-Aug-2016   | 1.2      | Changes as suggested by Kieran.
|  Akshay Nayak         | 28-Aug-2016   | 1.3      | Changes for defect 1455.
|  Akshay Nayak         | 2-Sep-2016    | 1.4      | Send valid BOM and Related Items.
|  Akshay Nayak         | 3-Sep-2016    | 1.5      | Performance improvement.
|  Akshay Nayak         | 17-Sep-2016    | 1.6      | Changes for generating Item Category and 
|                            category master data file and changes for GHEPMv2
|  Akshay Nayak         | 19-Sep-2016    | 1.7      | Integration changes
|  Prashanthi Bukkittu  | 27-Sep-2016    | 1.8      | Fixed the issue while generating CSV file for Item Attachment we are getting 
                                                      exception for one of the record
|  Akshay Nayak        | 30-Sep-2016     | 1.9      | Changed the delimiters from ~ to ,  
|            |         |        | There is a chance that data might contain comman. Such comma would be replaced with /,/ as escape sequence.
|  Akshay Nayak            |  5-Oct-2016     | 1.10     | Fix for temp space issue.
|  Akshay Nayak            |  12-Oct-2016     | 1.11     | Adding valuesetname to the relationship file
|  Prashanthi Bukkittu  | 18-Oct-2016    | 1.12      | Fixed the issue while generating CSV file for Item Relationship data for dff values
|  Prashanthi Bukkittu  | 20-Oct-2016    | 1.13      | Fixed the issue "CSV was extracting all the records instead of extracting the records in the given date range"
|  Akshay Nayak         | 20-Oct-2016    | 1.14      | Fix for issue identified by Betsy. Invalid ISBN value.
|  Prashanthi Bukkittu  | 24-Oct-2016    | 1.15      | Fixed the issue " no data getting populated in Relationship and Item Category master csv file"
|+----------------------+---------------+----------+--------------------------------------------+
|
+==============================================================================================*/

   gv_processed_flag      VARCHAR2 (1) := 'P'; 
    gv_item_entity         CONSTANT    VARCHAR2(20) := 'ITEM'; 
    gv_bom_entity         CONSTANT    VARCHAR2(20) := 'BOM';
    gv_related_item_entity     CONSTANT    VARCHAR2(20) := 'RELATED_ITEM';   
   
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
   * Narendra Mishra    1.0                                            23-Jun-2016
   ************************************************************************************* */    
    PROCEDURE Log_Msg(p_msg IN VARCHAR2)
    IS
        lc_msg  VARCHAR2 (4000) := p_msg;
    BEGIN
        IF gc_debug_flag = 'Y'
        THEN
            fnd_file.put_line (fnd_file.log, lc_msg);
            --dbms_output.put_line (lc_msg);
        END IF; 
    EXCEPTION WHEN OTHERS 
    THEN
        lc_msg := 'Unhandled exception in Log_Msg. Error: '||SQLCODE||'->'||SQLERRM;
        fnd_file.put_line (fnd_file.log, lc_msg);
        --dbms_output.put_line (lc_msg);
    END Log_Msg; 
    

/* ********************************************************
   * Procedure: Out_Msg
   *
   * Synopsis: This procedure is to print messages to the output file 
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
   * Narendra Mishra    1.0                                            23-Jun-2016
   ************************************************************************************* */    
    PROCEDURE Out_Msg(p_msg IN VARCHAR2)
    IS
        lc_msg  VARCHAR2 (4000) := p_msg;
    BEGIN
        fnd_file.put_line (fnd_file.output, lc_msg);
        --dbms_output.put_line (lc_msg);
    EXCEPTION WHEN OTHERS 
    THEN
        lc_msg := 'Unhandled exception in Out_Msg. Error: '||SQLCODE||'->'||SQLERRM;
        fnd_file.put_line (fnd_file.log, lc_msg);
        --dbms_output.put_line (lc_msg);
    END Out_Msg; 
    


/* ********************************************************
   * Procedure: populate_gtt
   *
   * Synopsis: This procedure populates inventory_item_id for modified items
   *         in this global temporary table.
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
   * Akshay Nayak        1.5                                            06-SEP-2016
   ************************************************************************************* */     
    
    PROCEDURE populate_gtt
    IS
    lv_count    NUMBER;
    BEGIN
    
    INSERT INTO XXPSO_EGO_ITEM_CHANGE_GTT (inventory_item_id) 
    SELECT inventory_item_id
      FROM apps.mtl_system_items_b
     WHERE last_update_date BETWEEN g_start_date AND g_end_date
     UNION
    SELECT inventory_item_id 
      FROM apps.ego_mtl_sy_items_ext_b 
     WHERE last_update_date BETWEEN g_start_date AND g_end_date
     UNION
    SELECT inventory_item_id 
      FROM apps.mtl_cross_references 
     WHERE last_update_date BETWEEN g_start_date AND g_end_date; 
    
    SELECT count(*)
      INTO lv_count
      FROM XXPSO_EGO_ITEM_CHANGE_GTT;
      
      Log_Msg('No of records in XXPSO_EGO_ITEM_CHANGE_GTT: '||lv_count);
    EXCEPTION
       WHEN OTHERS
       THEN
          Log_Msg('Error in populate_gtt - ' || SQLERRM);
    
    END;
    
--Changes for v1.11 Begin    
/* ********************************************************
* Procedure: get_rel_valueset_name
*
* Synopsis: Reads the setup and returns the valueset name attached to the segment 
* 	   present in global context 
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
* Akshay Nayak        1.11                                            12-OCT-2016
************************************************************************************* */     
FUNCTION get_rel_valueset_name(p_in_attr_name VARCHAR2)
RETURN VARCHAR2
IS 
lv_error_message 		VARCHAR2(4000);
lv_flex_value_set_name		VARCHAR2(100);
BEGIN
	
	BEGIN

            SELECT  ffvs.flex_value_set_name
              INTO  lv_flex_value_set_name
              FROM fnd_descriptive_flexs flex,
                   fnd_descriptive_flexs_tl flex_tl,
                   fnd_descr_flex_contexts flex_context,
                   fnd_descr_flex_column_usages flex_col_usage ,
                   fnd_flex_value_sets ffvs
             WHERE flex.application_id = flex_tl.application_id
               AND flex.descriptive_flexfield_name = flex_tl.descriptive_flexfield_name
               AND flex_tl.title = 'Item Relationships'
               AND flex.descriptive_flexfield_name = flex_context.descriptive_flexfield_name
               AND flex.application_id = flex_context.application_id
               AND flex_context.enabled_flag = 'Y'
               AND flex_context.global_flag = 'Y'
               AND flex_context.application_id = flex_col_usage.application_id(+)
               AND flex_context.descriptive_flexfield_name = flex_col_usage.descriptive_flexfield_name(+)
               AND flex_context.descriptive_flex_context_code = flex_col_usage.descriptive_flex_context_code(+)
               AND flex_col_usage.enabled_flag(+) = 'Y'
               AND flex_col_usage.flex_value_set_id =  ffvs.flex_value_set_id(+)
               AND flex_col_usage.application_column_name = p_in_attr_name;
        
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
        Log_Msg('No data found exception in get_rel_valueset_name for attribute name:'||p_in_attr_name);
        
	WHEN OTHERS THEN
	lv_error_message := lv_error_message||'~ Exception in get_rel_valueset_name while fetching value set name'||
					      ' :Error Message:'||SQLERRM;
	Log_Msg(lv_error_message);

        END;
        
	Log_Msg('lv_flex_value_set_name:'||lv_flex_value_set_name);
	RETURN lv_flex_value_set_name;

EXCEPTION
WHEN OTHERS THEN
lv_error_message := lv_error_message||'~ Exception in get_rel_valueset_name:Error Message:'||SQLERRM;
Log_Msg(lv_error_message);
RETURN NULL;
END get_rel_valueset_name;

--Changes for v1.11 End

/* ********************************************************
   * Procedure: generate_item_file
   *
   * Synopsis: This procedure is to generate item datafiles at the given path
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
   * Narendra Mishra    1.0                                            23-Jun-2016
   ************************************************************************************* */   
    PROCEDURE generate_item_file
    IS
        l_file          UTL_FILE.file_type;
        l_file_name     VARCHAR2(100)       := 'XXPSO_ITEM_DATA'; 
        l_rec_count     NUMBER              := 0;
        l_suc_count     NUMBER              := 0;
        
        CURSOR c_item
        IS
              SELECT msib.inventory_item_id             inventory_item_id    --Changes for v1.5 msie.inventory_item_id
                    ,msib.segment1                          item_number        --Changes for v1.5 msie.item
                    ,msib.organization_id               organization_id
                    ,mp.organization_code                 organization_code        --Changes for v1.5 msie.organization_code 
                    ,hou.name                      organization_name    --Changes for v1.5 msie.organization_name
                    ,cat.concatenated_segments          item_catalog_group    --Changes for v1.5 msie.item_catalog_group
                    ,mstl.description                  item_description    --Changes for v1.5 msie.item_description
                    ,mstl.long_description              long_description    --Changes for v1.5 msie.long_description
                    ,msib.inventory_item_status_code    inventory_item_status    --Changes for v1.5 msie.inventory_item_status_code
                    ,apps.INV_MEANING_SEL.c_fndcommon(msib.ITEM_TYPE,'ITEM_TYPE')
                                        item_type        --Changes for v1.5 msie.item_type 
                    ,apps.INV_MEANING_SEL.c_unitmeasure(MSIb.PRIMARY_UOM_CODE)
                                        primary_unit_of_measure    --Changes for v1.5 msie.primary_unit_of_measure
                    ,pel.name                           lifecycle
                    ,pelp.name                          lifecycle_phase
                    ,msib.approval_status               approval_status
                    ,msib.weight_uom_code               weight_uom_code
                    ,msib.unit_weight                   unit_weight
                    ,msib.volume_uom_code               volume_uom_code
                    ,msib.unit_volume                   unit_volume
                    ,msib.dimension_uom_code            dimension_uom_code
                    ,msib.unit_length                   unit_length
                    ,msib.unit_width                    unit_width
                    ,msib.unit_height                   unit_height
                    ,fu1.user_name                      created_by_name
                    ,msib.creation_date                 creation_date        
                    ,fu2.user_name                      last_updated_by_name
                    ,msib.last_update_date              last_update_date
                FROM --apps.mtl_system_items_er1_v        msie    --Changes for v1.5
                    apps.mtl_system_items_b            msib
                    ,apps.mtl_system_items_tl          mstl    --Changes for v1.5
                    ,apps.mtl_parameters               mp    --Changes for v1.5
                    ,apps.hr_organization_units        hou    --Changes for v1.5
                    ,mtl_item_catalog_groups_kfv        cat     --Changes for v1.5                   
                    ,apps.fnd_user                      fu1
                    ,apps.fnd_user                      fu2
                    ,apps.pa_ego_lifecycles_v           pel
                    ,apps.pa_ego_lifecycles_phases_v    pelp
                    ,apps.xxpso_ego_item_change_gtt    item_gtt
              WHERE 1=1
                --AND msie.inventory_item_id          = msib.inventory_item_id    --Changes for v1.5
                --AND msie.organization_id            = msib.organization_id    --Changes for v1.5
                AND msib.enabled_flag               = 'Y'
                AND mstl.inventory_item_id          = msib.inventory_item_id
                AND mstl.organization_id            = msib.organization_id
                AND msib.organization_id            = mp.organization_id
                AND mp.organization_id              = hou.organization_id
                AND msib.item_catalog_group_id      = cat.item_catalog_group_id
                AND msib.created_by                 = fu1.user_id 
                AND msib.last_updated_by            = fu2.user_id 
                AND msib.lifecycle_id               = pel.proj_element_id(+)
                AND pel.object_type(+)              = 'PA_STRUCTURES' 
                AND msib.lifecycle_id               = pelp.parent_structure_id(+)
                AND msib.current_phase_id           = pelp.proj_element_id(+)
                AND pelp.object_type(+)             ='PA_TASKS' 
                --AND msib.last_update_date         BETWEEN g_start_date AND g_end_date;
                --Changes for v1.5
                AND msib.inventory_item_id         = item_gtt.inventory_item_id
                /*
                AND msib.inventory_item_id          IN 
                    (SELECT inventory_item_id
                       FROM apps.mtl_system_items_b
                      WHERE last_update_date BETWEEN g_start_date AND g_end_date
                      UNION
                     SELECT inventory_item_id 
                       FROM apps.ego_mtl_sy_items_ext_b 
                      WHERE last_update_date BETWEEN g_start_date AND g_end_date
                    )*/
           ORDER BY msib.inventory_item_id;
           
       l_rec_items_rec            xxpso_ego_items_stg%ROWTYPE;
       lv_item_record_found        VARCHAR2(1);
       lv_originating_system        VARCHAR2(10);
       lv_originating_system_ref     VARCHAR2(255);  
       lv_originating_system_date     DATE;  
       lv_transaction_type        VARCHAR2(10);
           
    BEGIN
    
        Log_Msg(gcn_print_line);
        Log_Msg('Start of generate_item_file and generate_uda_file Procedure');
    
        l_file_name := l_file_name ||'_'|| gcn_request_id ||'_'|| g_sysdate ||'.csv';

        
        Log_Msg('DBA Directory Name - ' || gc_dba_directory_name);
        Log_Msg('File Name  - ' || l_file_name);
        
        --Changes for version 1.8 start
        l_file := UTL_FILE.fopen (gc_dba_directory_name, l_file_name, 'w',32767);
    --Changes for version 1.8 end

        
        UTL_FILE.put_line(  l_file,
            --Header related parameters
                           'OrigSystem'            
                ||','||    'OrigSystemReference'   
                ||','||    'OrigSystemTimeStamp'   
                ||','||    'MDMReference'          
                ||','||    'RecordType'            
                ||','||    'Entity'              
                -- Entity specific parameters
            ||','||   'ItemId' 
                ||','||    'ItemNumber'                       
                ||','||    'OrganizationCode'        
                ||','||    'OrganizationName'        
                ||','||    'CatalogCategory'         
                ||','||    'Description'             
                ||','||    'LongDescription'         
                ||','||    'PrimaryUnitOfMeasure'    
                ||','||    'ItemStatus'              
                ||','||    'UserItemType'            
                ||','||    'LifeCycle'               
                ||','||    'LifeCyclePhase'          
                ||','||    'ApprovalStatus'                        
                ||','||    'WeightUomCode'          
                ||','||    'UnitWeight'             
                ||','||    'VolumeUomCode'          
                ||','||    'UnitVolume'             
                ||','||    'DimensionUomCode'       
                ||','||    'UnitLength'             
                ||','||    'UnitWidth'              
                ||','||    'UnitHeight'             
                ||','||    'CreationDate'            
                ||','||    'CreatedBy'               
                ||','||    'LastUpdateDate'          
                ||','||    'LastUpdatedBy'  
                ,TRUE --Changes for v1.8
                             );                         
        
    Log_Msg('Start Item Inner loop: '||to_char(SYSDATE,'DD-MM-YYYY HH24:MI:SS'));      
        FOR c IN c_item
        LOOP
            l_rec_count := l_rec_count + 1;
            lv_item_record_found := 'N';

                --Get the last updated record.
                BEGIN
        Log_Msg('Fetching Integration item information for inventory_item_id:'||c.inventory_item_id||
                ' organization_id:'||c.organization_id);
        
        SELECT * 
          INTO l_rec_items_rec
          FROM xxpso_ego_items_stg stg1
         WHERE stg1.inventory_item_id = c.inventory_item_id
           AND stg1.organization_id = c.organization_id
                   AND stg1.status_code = gv_processed_flag
                   AND stg1.published_by_etl = 'N'
                   AND stg1.last_update_date = (select max(stg2.last_update_date)
                                    FROM  xxpso_ego_items_stg stg2
                                    WHERE stg2.inventory_item_id = stg1.inventory_item_id
                                      AND stg2.organization_id = stg1.organization_id
                          AND stg2.status_code = gv_processed_flag
                          AND stg2.published_by_etl = 'N'                          
                       );
        
                Log_Msg('Item record found: '||l_rec_items_rec.record_id);
                lv_item_record_found := 'Y';
                
                EXCEPTION
                WHEN OTHERS THEN
                lv_item_record_found := 'N';
                END;
                
                IF lv_item_record_found = 'Y' THEN
                    lv_originating_system := l_rec_items_rec.source_system;
                    lv_originating_system_ref := l_rec_items_rec.source_item_number;
                    lv_originating_system_date := l_rec_items_rec.last_update_date;
                    IF l_rec_items_rec.transaction_type = 'CREATE' THEN
                    	lv_transaction_type       := 'C';
                    ELSIF l_rec_items_rec.transaction_type = 'UPDATE' THEN
                    	lv_transaction_type       := 'U';
                    END IF;
                    --lv_transaction_type       := l_rec_items_rec.transaction_type;
                    Log_Msg('Record Id of matched Item:'||l_rec_items_rec.record_id);                    
                ELSE
                    lv_originating_system := 'MDM';
                    lv_originating_system_date := SYSDATE;
                    lv_originating_system_ref  := NULL;--Changes for v1.14
                    lv_transaction_type	       := NULL;--Changes for v1.14
                END IF;  
                
            BEGIN
                UTL_FILE.put_line(  l_file,
                    -- Header specific parameters
                                  replace( lv_originating_system  ,',','~,')       
                        ||','||   replace( lv_originating_system_ref ,',','~,') 
                        ||','||   replace( to_char(lv_originating_system_date,'DD-MM-YYYY HH24:MI:SS')  ,',','~,') 
                        ||','||   replace( c.inventory_item_id    ,',','~,')    
                        ||','||   replace( lv_transaction_type  ,',','~,')      
                        ||','||   replace( gv_item_entity ,',','~,')        
                        -- Entity specific parameters
                        ||','||   replace( c.inventory_item_id  ,',','~,')
                        ||','||   replace( c.item_number  ,',','~,')                            
                        ||','||   replace( c.organization_code ,',','~,')          
                        ||','||   replace( c.organization_name ,',','~,')          
                        ||','||   replace( c.item_catalog_group  ,',','~,')        
                        ||','||   replace( c.item_description  ,',','~,')          
                        ||','||   replace( c.long_description   ,',','~,')         
                        ||','||   replace( c.primary_unit_of_measure ,',','~,')    
                        ||','||   replace( c.inventory_item_status ,',','~,')      
                        ||','||   replace( c.item_type ,',','~,')                  
                        ||','||   replace( c.lifecycle  ,',','~,')                 
                        ||','||   replace( c.lifecycle_phase ,',','~,')            
                        ||','||   replace( c.approval_status ,',','~,')                                 
                        ||','||   replace( c.weight_uom_code ,',','~,')          
                        ||',' ||  replace(  c.unit_weight ,',','~,')               
                        ||','||   replace( c.volume_uom_code ,',','~,')                     
                        ||',' ||  replace(  c.unit_volume ,',','~,')                
                        ||','||   replace( c.dimension_uom_code ,',','~,')                     
                        ||',' ||  replace(  c.unit_length ,',','~,')                       
                        ||',' ||  replace(  c.unit_width   ,',','~,')                    
                        ||',' ||  replace(  c.unit_height ,',','~,')     
                        ||','||   replace( to_char(c.creation_date , 'DD-MM-YYYY HH24:MI:SS') ,',','~,')             
                        ||','||   replace( c.created_by_name ,',','~,')            
                        ||','||   replace( to_char(c.last_update_date , 'DD-MM-YYYY HH24:MI:SS') ,',','~,')         
                        ||','||   replace( c.last_updated_by_name  ,',','~,')      
                        ,TRUE --Changes for v1.8
                            );
                                 
                l_suc_count := l_suc_count + 1;
                
        --After getting the last updated record update     published_by_etl flag to Y for all earlier and exising records.           
        UPDATE xxpso_ego_items_stg
           SET published_by_etl = 'Y'
         WHERE inventory_item_id = c.inventory_item_id
           AND organization_id = c.organization_id
           AND status_code = gv_processed_flag
                   AND published_by_etl = 'N';                
            EXCEPTION
                WHEN OTHERS
                THEN
                    Log_Msg('Unable to write to file for Items with Inventory Item Id:'||c.inventory_item_id||
                    '- ' || SQLERRM);
            END;
        END LOOP;

  
        
        
        IF l_rec_count = l_suc_count
        THEN
            Log_Msg('Item File generated successfully');
            
            -- Printing the end of file line  
            UTL_FILE.put_line(l_file, 'Total No of Records - ' ||','|| l_suc_count );
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Success');
        ELSE    
            Log_Msg('Error while generating Item File');
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Failed');
        END IF; 
                
        
        UTL_FILE.fclose (l_file);
        
        Log_Msg(' ');
        Log_Msg('End of generate_item_file Procedure');
        Log_Msg(gcn_print_line); 
                
        -- Writing to Output File 
        Out_Msg(gcn_print_line); 
        Out_Msg('No of records pulled from database for Item            : '|| l_rec_count);
        Out_Msg('No of records inserted into Item CSV File              : '|| l_suc_count);
        Out_Msg(gcn_print_line); 
        Out_Msg(' ');
        
        
    EXCEPTION
       WHEN OTHERS
       THEN
          Log_Msg('Error in generate_item_file procedure - ' || SQLERRM);
    
    END generate_item_file;
    
    

/* ********************************************************
   * Procedure: generate_uda_file
   *
   * Synopsis: This procedure is to generate UDA datafiles at the given path
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
   * Narendra Mishra    1.0                                            24-Jun-2016
   ************************************************************************************* */   
    PROCEDURE generate_uda_file
    IS
        l_file          UTL_FILE.file_type;
        l_file_name     VARCHAR2(100)    := 'XXPSO_UDA_DATA';
        l_rec_count     NUMBER              := 0;
        l_suc_count     NUMBER              := 0;        
        v_attr_value    VARCHAR2 (2000);
        
        CURSOR c_uda
        IS
              SELECT ego.inventory_item_id              inventory_item_id
                    ,ego.extension_id                   extension_id
                    --Changes for 1.10 Begin
                    --Change for v1.5 Begin
                    /*
                    ,ego.c_ext_attr1            c_ext_attr1
                    ,ego.c_ext_attr2            c_ext_attr2
                    ,ego.c_ext_attr3            c_ext_attr3
                    ,ego.c_ext_attr4            c_ext_attr4
                    ,ego.c_ext_attr5            c_ext_attr5
                    ,ego.c_ext_attr6            c_ext_attr6
                    ,ego.c_ext_attr7            c_ext_attr7
                    ,ego.c_ext_attr8            c_ext_attr8
                    ,ego.c_ext_attr9            c_ext_attr9
                    ,ego.c_ext_attr10            c_ext_attr10
                    ,ego.c_ext_attr11            c_ext_attr11
                    ,ego.c_ext_attr12            c_ext_attr12
                    ,ego.c_ext_attr13            c_ext_attr13
                    ,ego.c_ext_attr14            c_ext_attr14
                    ,ego.c_ext_attr15            c_ext_attr15
                    ,ego.c_ext_attr16            c_ext_attr16
                    ,ego.c_ext_attr17            c_ext_attr17
                    ,ego.c_ext_attr18            c_ext_attr18
                    ,ego.c_ext_attr19            c_ext_attr19
                    ,ego.c_ext_attr20            c_ext_attr20
                    ,ego.c_ext_attr21            c_ext_attr21
                    ,ego.c_ext_attr22            c_ext_attr22
                    ,ego.c_ext_attr23            c_ext_attr23
                    ,ego.c_ext_attr24            c_ext_attr24
                    ,ego.c_ext_attr25            c_ext_attr25
                    ,ego.c_ext_attr25            c_ext_attr26
                    ,ego.c_ext_attr26            c_ext_attr27
                    ,ego.c_ext_attr27            c_ext_attr28
                    ,ego.c_ext_attr29            c_ext_attr29
                    ,ego.c_ext_attr30            c_ext_attr30
                    ,ego.c_ext_attr31            c_ext_attr31
                    ,ego.c_ext_attr32            c_ext_attr32
                    ,ego.c_ext_attr33            c_ext_attr33
                    ,ego.c_ext_attr34            c_ext_attr34
                    ,ego.c_ext_attr35            c_ext_attr35
                    ,ego.c_ext_attr36            c_ext_attr36
                    ,ego.c_ext_attr37            c_ext_attr37
                    ,ego.c_ext_attr38            c_ext_attr38
                    ,ego.c_ext_attr39            c_ext_attr39
                    ,ego.c_ext_attr40            c_ext_attr40
                    ,ego.n_ext_attr1            n_ext_attr1
                    ,ego.n_ext_attr2            n_ext_attr2
                    ,ego.n_ext_attr3            n_ext_attr3
                    ,ego.n_ext_attr4            n_ext_attr4
                    ,ego.n_ext_attr5            n_ext_attr5
                    ,ego.n_ext_attr6            n_ext_attr6
                    ,ego.n_ext_attr7            n_ext_attr7
                    ,ego.n_ext_attr8            n_ext_attr8
                    ,ego.n_ext_attr9            n_ext_attr9
                    ,ego.n_ext_attr10            n_ext_attr10
                    ,ego.n_ext_attr11            n_ext_attr11
                    ,ego.n_ext_attr12            n_ext_attr12
                    ,ego.n_ext_attr13            n_ext_attr13
                    ,ego.n_ext_attr14            n_ext_attr14
                    ,ego.n_ext_attr15            n_ext_attr15
                    ,ego.n_ext_attr16            n_ext_attr16
                    ,ego.n_ext_attr17            n_ext_attr17
                    ,ego.n_ext_attr18            n_ext_attr18
                    ,ego.n_ext_attr19            n_ext_attr19
                    ,ego.n_ext_attr20            n_ext_attr20
                    ,ego.uom_ext_attr1            uom_ext_attr1
                    ,ego.uom_ext_attr2            uom_ext_attr2
                    ,ego.uom_ext_attr3            uom_ext_attr3
                    ,ego.uom_ext_attr4            uom_ext_attr4
                    ,ego.uom_ext_attr5            uom_ext_attr5
                    ,ego.uom_ext_attr6            uom_ext_attr6
                    ,ego.uom_ext_attr7            uom_ext_attr7
                    ,ego.uom_ext_attr8            uom_ext_attr8
                    ,ego.uom_ext_attr9            uom_ext_attr9
                    ,ego.uom_ext_attr10            uom_ext_attr10
                    ,ego.uom_ext_attr11            uom_ext_attr11
                    ,ego.uom_ext_attr12            uom_ext_attr12
                    ,ego.uom_ext_attr13            uom_ext_attr13
                    ,ego.uom_ext_attr14            uom_ext_attr14
                    ,ego.uom_ext_attr15            uom_ext_attr15
                    ,ego.uom_ext_attr16            uom_ext_attr16
                    ,ego.uom_ext_attr17            uom_ext_attr17
                    ,ego.uom_ext_attr18            uom_ext_attr18
                    ,ego.uom_ext_attr19            uom_ext_attr19
                    ,ego.uom_ext_attr20            uom_ext_attr20  
                    ,ego.d_ext_attr1            d_ext_attr1
                    ,ego.d_ext_attr2            d_ext_attr2
                    ,ego.d_ext_attr3            d_ext_attr3
                    ,ego.d_ext_attr4            d_ext_attr4
                    ,ego.d_ext_attr5            d_ext_attr5
                    ,ego.d_ext_attr6            d_ext_attr6
                    ,ego.d_ext_attr7            d_ext_attr7
                    ,ego.d_ext_attr8            d_ext_attr8
                    ,ego.d_ext_attr9            d_ext_attr9
                    ,ego.d_ext_attr10            d_ext_attr10   
                    ,ego.tl_ext_attr1            tl_ext_attr1
                    ,ego.tl_ext_attr2            tl_ext_attr2
                    ,ego.tl_ext_attr3            tl_ext_attr3
                    ,ego.tl_ext_attr4            tl_ext_attr4
                    ,ego.tl_ext_attr5            tl_ext_attr5
                    ,ego.tl_ext_attr6            tl_ext_attr6
                    ,ego.tl_ext_attr7            tl_ext_attr7
                    ,ego.tl_ext_attr8            tl_ext_attr8
                    ,ego.tl_ext_attr9            tl_ext_attr9
                    ,ego.tl_ext_attr10            tl_ext_attr10
                    ,ego.tl_ext_attr11            tl_ext_attr11
                    ,ego.tl_ext_attr12            tl_ext_attr12
                    ,ego.tl_ext_attr13            tl_ext_attr13
                    ,ego.tl_ext_attr14            tl_ext_attr14
                    ,ego.tl_ext_attr15            tl_ext_attr15
                    ,ego.tl_ext_attr16            tl_ext_attr16
                    ,ego.tl_ext_attr17            tl_ext_attr17
                    ,ego.tl_ext_attr18            tl_ext_attr18
                    ,ego.tl_ext_attr19            tl_ext_attr19
                    ,ego.tl_ext_attr20            tl_ext_attr20
                    ,ego.tl_ext_attr21            tl_ext_attr21
                    ,ego.tl_ext_attr22            tl_ext_attr22
                    ,ego.tl_ext_attr23            tl_ext_attr23
                    ,ego.tl_ext_attr24            tl_ext_attr24
                    ,ego.tl_ext_attr25            tl_ext_attr25
                    ,ego.tl_ext_attr25            tl_ext_attr26
                    ,ego.tl_ext_attr26            tl_ext_attr27
                    ,ego.tl_ext_attr27            tl_ext_attr28
                    ,ego.tl_ext_attr29            tl_ext_attr29
                    ,ego.tl_ext_attr30            tl_ext_attr30
                    ,ego.tl_ext_attr31            tl_ext_attr31
                    ,ego.tl_ext_attr32            tl_ext_attr32
                    ,ego.tl_ext_attr33            tl_ext_attr33
                    ,ego.tl_ext_attr34            tl_ext_attr34
                    ,ego.tl_ext_attr35            tl_ext_attr35
                    ,ego.tl_ext_attr36            tl_ext_attr36
                    ,ego.tl_ext_attr37            tl_ext_attr37
                    ,ego.tl_ext_attr38            tl_ext_attr38
                    ,ego.tl_ext_attr39            tl_ext_attr39
                    ,ego.tl_ext_attr40            tl_ext_attr40  
                        */
                    --Changes for v1.5 End
                    --Changes for 1.10 End
                    ,DECODE(atg.multi_row_code,'Y',ego.extension_id,NULL)
                                                        Group_Identifier
                    ,atg.attr_group_name                attr_group_name
                    ,atg.attr_group_disp_name           attr_group_disp_name
                    ,atg.multi_row_code                 multi_row_code
                    ,att.attr_name                      attr_name
                    ,att.attr_display_name              attr_display_name
                    ,att.database_column                database_column      
                    ,SUBSTR(att.database_column,INSTR(att.database_column,'ATTR')+4) column_number
                    ,att.data_type_code                 data_type_code
                    ,ffvs.flex_value_set_name        flex_value_set_name
                    ,fu1.user_name                      created_by_name
                    ,ego.creation_date                  creation_date        
                    ,fu2.user_name                      last_updated_by_name
                    ,ego.last_update_date               last_update_date
               FROM apps.ego_mtl_sy_items_ext_vl         ego
                    ,apps.xxpso_ego_attr_groups_v        atg
                    ,apps.xxpso_ego_attrs_v              att
                    ,apps.fnd_user                       fu1
                    ,apps.fnd_user                       fu2
                    ,apps.xxpso_ego_item_change_gtt    item_gtt 
                    ,apps.mtl_parameters        mp
                    ,apps.fnd_flex_value_sets ffvs
              WHERE ego.attr_group_id               = atg.attr_group_id
                AND att.attr_group_type             = atg.attr_group_type
                AND att.attr_group_name             = atg.attr_group_name
                AND att.enabled_flag                = 'Y'
                AND atg.application_id              = 431
                AND att.application_id              = 431
                AND ego.created_by                  = fu1.user_id 
                AND ego.last_updated_by             = fu2.user_id 
                --Changes for v1.5
                AND ego.inventory_item_id         = item_gtt.inventory_item_id 
                AND ego.organization_id         = mp.organization_id
                AND mp.organization_code        = 'MDM'
                AND att.value_set_id                = ffvs.flex_value_set_id(+) 
                --AND ego.last_update_date      BETWEEN g_start_date AND g_end_date;
                /*
                AND ego.inventory_item_id IN 
                    (SELECT inventory_item_id
                       FROM apps.mtl_system_items_b
                      WHERE last_update_date BETWEEN g_start_date AND g_end_date
                      UNION
                     SELECT inventory_item_id 
                       FROM apps.ego_mtl_sy_items_ext_b 
                      WHERE last_update_date BETWEEN g_start_date AND g_end_date
                    )*/
           ORDER BY ego.inventory_item_id, ego.extension_id, att.attr_name;
 
        TYPE l_uda_tab_type IS TABLE OF             c_uda%ROWTYPE INDEX BY BINARY_INTEGER;
        l_uda_tab_tbl                               l_uda_tab_type; 
        
        --Changes for 1.10 Begin
        lv_c_ext_attr1            VARCHAR2(150);
        lv_c_ext_attr2            VARCHAR2(150);
        lv_c_ext_attr3            VARCHAR2(150);
        lv_c_ext_attr4            VARCHAR2(150);
        lv_c_ext_attr5            VARCHAR2(150);
        lv_c_ext_attr6            VARCHAR2(150);
        lv_c_ext_attr7            VARCHAR2(150);
        lv_c_ext_attr8            VARCHAR2(150);
        lv_c_ext_attr9            VARCHAR2(150);
        lv_c_ext_attr10            VARCHAR2(150);
        lv_c_ext_attr11            VARCHAR2(150);
        lv_c_ext_attr12            VARCHAR2(150);
        lv_c_ext_attr13            VARCHAR2(150);
        lv_c_ext_attr14            VARCHAR2(150);
        lv_c_ext_attr15            VARCHAR2(150);
        lv_c_ext_attr16            VARCHAR2(150);
        lv_c_ext_attr17            VARCHAR2(150);
        lv_c_ext_attr18            VARCHAR2(150);
        lv_c_ext_attr19            VARCHAR2(150);
        lv_c_ext_attr20            VARCHAR2(150);
        lv_c_ext_attr21            VARCHAR2(150);
        lv_c_ext_attr22            VARCHAR2(150);
        lv_c_ext_attr23            VARCHAR2(150);
        lv_c_ext_attr24            VARCHAR2(150);
        lv_c_ext_attr25            VARCHAR2(150);
        lv_c_ext_attr26            VARCHAR2(150);
        lv_c_ext_attr27            VARCHAR2(150);
        lv_c_ext_attr28            VARCHAR2(150);
        lv_c_ext_attr29            VARCHAR2(150);
        lv_c_ext_attr30            VARCHAR2(150);
        lv_c_ext_attr31            VARCHAR2(150);
        lv_c_ext_attr32            VARCHAR2(150);
        lv_c_ext_attr33            VARCHAR2(150);
        lv_c_ext_attr34            VARCHAR2(150);
        lv_c_ext_attr35            VARCHAR2(150);
        lv_c_ext_attr36            VARCHAR2(150);
        lv_c_ext_attr37            VARCHAR2(150);
        lv_c_ext_attr38            VARCHAR2(150);
        lv_c_ext_attr39            VARCHAR2(150);
        lv_c_ext_attr40            VARCHAR2(150);
        ln_n_ext_attr1            NUMBER;
        ln_n_ext_attr2            NUMBER;        
        ln_n_ext_attr3            NUMBER;
        ln_n_ext_attr4            NUMBER;
        ln_n_ext_attr5            NUMBER;
        ln_n_ext_attr6            NUMBER;
        ln_n_ext_attr7            NUMBER;
        ln_n_ext_attr8            NUMBER;
        ln_n_ext_attr9            NUMBER;
        ln_n_ext_attr10            NUMBER;
        ln_n_ext_attr11            NUMBER;
        ln_n_ext_attr12            NUMBER;
        ln_n_ext_attr13            NUMBER;
        ln_n_ext_attr14            NUMBER;
        ln_n_ext_attr15            NUMBER;
        ln_n_ext_attr16            NUMBER;    
        ln_n_ext_attr17            NUMBER;
        ln_n_ext_attr18            NUMBER;
        ln_n_ext_attr19            NUMBER;
        ln_n_ext_attr20            NUMBER;
        lv_uom_ext_attr1        VARCHAR2(3);
        lv_uom_ext_attr2        VARCHAR2(3);
        lv_uom_ext_attr3        VARCHAR2(3);
        lv_uom_ext_attr4        VARCHAR2(3);
        lv_uom_ext_attr5        VARCHAR2(3);
        lv_uom_ext_attr6        VARCHAR2(3);
        lv_uom_ext_attr7        VARCHAR2(3);
        lv_uom_ext_attr8        VARCHAR2(3);
        lv_uom_ext_attr9        VARCHAR2(3);
        lv_uom_ext_attr10        VARCHAR2(3);
        lv_uom_ext_attr11        VARCHAR2(3);
        lv_uom_ext_attr12        VARCHAR2(3);
        lv_uom_ext_attr13        VARCHAR2(3);
        lv_uom_ext_attr14        VARCHAR2(3);
        lv_uom_ext_attr15        VARCHAR2(3);
        lv_uom_ext_attr16        VARCHAR2(3);
        lv_uom_ext_attr17        VARCHAR2(3);
        lv_uom_ext_attr18        VARCHAR2(3);
        lv_uom_ext_attr19        VARCHAR2(3);
        lv_uom_ext_attr20          VARCHAR2(3);
        ld_d_ext_attr1            DATE;
        ld_d_ext_attr2            DATE;
        ld_d_ext_attr3            DATE;
        ld_d_ext_attr4            DATE;
        ld_d_ext_attr5            DATE;
        ld_d_ext_attr6            DATE;
        ld_d_ext_attr7            DATE;
        ld_d_ext_attr8            DATE;
        ld_d_ext_attr9            DATE;
        ld_d_ext_attr10           DATE;
        lv_tl_ext_attr1            VARCHAR2(1000);
        lv_tl_ext_attr2            VARCHAR2(1000);
        lv_tl_ext_attr3            VARCHAR2(1000);
        lv_tl_ext_attr4            VARCHAR2(1000);
        lv_tl_ext_attr5            VARCHAR2(1000);
        lv_tl_ext_attr6            VARCHAR2(1000);
        lv_tl_ext_attr7            VARCHAR2(1000);
        lv_tl_ext_attr8            VARCHAR2(1000);
        lv_tl_ext_attr9            VARCHAR2(1000);
        lv_tl_ext_attr10        VARCHAR2(1000);
        lv_tl_ext_attr11        VARCHAR2(1000);
        lv_tl_ext_attr12        VARCHAR2(1000);
        lv_tl_ext_attr13        VARCHAR2(1000);
        lv_tl_ext_attr14        VARCHAR2(1000);
        lv_tl_ext_attr15        VARCHAR2(1000);
        lv_tl_ext_attr16        VARCHAR2(1000);
        lv_tl_ext_attr17        VARCHAR2(1000);
        lv_tl_ext_attr18        VARCHAR2(1000);
        lv_tl_ext_attr19        VARCHAR2(1000);
        lv_tl_ext_attr20        VARCHAR2(1000);
        lv_tl_ext_attr21        VARCHAR2(1000);
        lv_tl_ext_attr22        VARCHAR2(1000);
        lv_tl_ext_attr23        VARCHAR2(1000);
        lv_tl_ext_attr24        VARCHAR2(1000);    
        lv_tl_ext_attr25        VARCHAR2(1000);
        lv_tl_ext_attr26        VARCHAR2(1000);
        lv_tl_ext_attr27        VARCHAR2(1000);
        lv_tl_ext_attr28        VARCHAR2(1000);
        lv_tl_ext_attr29        VARCHAR2(1000);
        lv_tl_ext_attr30        VARCHAR2(1000);
        lv_tl_ext_attr31        VARCHAR2(1000);
        lv_tl_ext_attr32        VARCHAR2(1000);
        lv_tl_ext_attr33        VARCHAR2(1000);
        lv_tl_ext_attr34        VARCHAR2(1000);
        lv_tl_ext_attr35        VARCHAR2(1000);
        lv_tl_ext_attr36        VARCHAR2(1000);
        lv_tl_ext_attr37        VARCHAR2(1000);    
        lv_tl_ext_attr38        VARCHAR2(1000);
        lv_tl_ext_attr39        VARCHAR2(1000);
        lv_tl_ext_attr40         VARCHAR2(1000);
        --Changes for 1.10 End
                  
    BEGIN
    
        Log_Msg(gcn_print_line);
        Log_Msg('Start of generate_uda_file Procedure');
    
        l_file_name := l_file_name||'_'||TO_CHAR(gcn_request_id)||'_'||g_sysdate||'.csv';
        
        Log_Msg('DBA Directory Name  - ' || gc_dba_directory_name);
        Log_Msg('File Name  - ' || l_file_name);
        
        --Changes for version 1.8 start
        l_file := UTL_FILE.fopen (gc_dba_directory_name, l_file_name, 'w',32767);
    --Changes for version 1.8 end
        
        UTL_FILE.put_line(  l_file,
                           'ItemId'                              
                ||','||    'AttrGroupInternalName'   
                ||','||    'AttrGroupDisplayName'     
                ||','||    'MultiRowFlag'             
                ||','||    'GroupIdentifier'        
                ||','||    'AttrInternalName'        
                ||','||    'AttrDisplayName'         
                ||','||    'AttrDataType'            
                ||','||    'AttrValue'              
                ||','||    'ValueSetName'          
                ||','||    'CreationDate'            
                ||','||    'CreatedBy'               
                ||','||    'LastUpdateDate'          
                ||','||    'LastUpdatedBy'  
                ,TRUE --Changes for v1.8
                         );
        
        --FOR c IN c_uda
      OPEN c_uda;
      LOOP
      FETCH c_uda
      BULK COLLECT INTO l_uda_tab_tbl LIMIT 100;
      EXIT WHEN l_uda_tab_tbl.count = 0;
        FOR index_count IN 1..l_uda_tab_tbl.count
        LOOP
          v_attr_value    := NULL;
          BEGIN
            /*
                EXECUTE IMMEDIATE  'SELECT TO_CHAR( '|| l_uda_tab_tbl(index_count).database_column ||' ) FROM ego_mtl_sy_items_ext_vl where inventory_item_id = '
                           || l_uda_tab_tbl(index_count).inventory_item_id || ' AND extension_id = ' || l_uda_tab_tbl(index_count).extension_id || ''
                INTO v_attr_value;*/
                --Changes for 1.10 Begin
        BEGIN
                    SELECT c_ext_attr1,c_ext_attr2,c_ext_attr3,c_ext_attr4,c_ext_attr5
                          ,c_ext_attr6,c_ext_attr7,c_ext_attr8,c_ext_attr9,c_ext_attr10
                          ,c_ext_attr11,c_ext_attr12,c_ext_attr13,c_ext_attr14,c_ext_attr15
                          ,c_ext_attr16,c_ext_attr17,c_ext_attr18,c_ext_attr19,c_ext_attr20
                          ,c_ext_attr21,c_ext_attr22,c_ext_attr23,c_ext_attr24,c_ext_attr25
                          ,c_ext_attr26,c_ext_attr27,c_ext_attr28,c_ext_attr29,c_ext_attr30    
                          ,c_ext_attr31,c_ext_attr32,c_ext_attr33,c_ext_attr34,c_ext_attr35
                          ,c_ext_attr36,c_ext_attr37,c_ext_attr38,c_ext_attr39,c_ext_attr40
                          ,n_ext_attr1,n_ext_attr2,n_ext_attr3,n_ext_attr4,n_ext_attr5
                          ,n_ext_attr6,n_ext_attr7,n_ext_attr8,n_ext_attr9,n_ext_attr10    
                          ,n_ext_attr11,n_ext_attr12,n_ext_attr13,n_ext_attr14,n_ext_attr15
                          ,n_ext_attr16,n_ext_attr17,n_ext_attr18,n_ext_attr19,n_ext_attr20    
                          ,uom_ext_attr1,uom_ext_attr2,uom_ext_attr3,uom_ext_attr4,uom_ext_attr5    
                          ,uom_ext_attr6,uom_ext_attr7,uom_ext_attr8,uom_ext_attr9,uom_ext_attr10
                          ,uom_ext_attr11,uom_ext_attr12,uom_ext_attr13,uom_ext_attr14,uom_ext_attr15                          
                          ,uom_ext_attr16,uom_ext_attr17,uom_ext_attr18,uom_ext_attr19,uom_ext_attr20
                          ,d_ext_attr1,d_ext_attr2,d_ext_attr3,d_ext_attr4,d_ext_attr5
                          ,d_ext_attr6,d_ext_attr7,d_ext_attr8,d_ext_attr9,d_ext_attr10
                          ,tl_ext_attr1,tl_ext_attr2,tl_ext_attr3,tl_ext_attr4,tl_ext_attr5
                          ,tl_ext_attr6,tl_ext_attr7,tl_ext_attr8,tl_ext_attr9,tl_ext_attr10
                          ,tl_ext_attr11,tl_ext_attr12,tl_ext_attr13,tl_ext_attr14,tl_ext_attr15
                          ,tl_ext_attr16,tl_ext_attr17,tl_ext_attr18,tl_ext_attr19,tl_ext_attr20
                          ,tl_ext_attr21,tl_ext_attr22,tl_ext_attr23,tl_ext_attr24,tl_ext_attr25
                          ,tl_ext_attr26,tl_ext_attr27,tl_ext_attr28,tl_ext_attr29,tl_ext_attr30
                          ,tl_ext_attr31,tl_ext_attr32,tl_ext_attr33,tl_ext_attr34,tl_ext_attr35
                          ,tl_ext_attr36,tl_ext_attr37,tl_ext_attr38,tl_ext_attr39,tl_ext_attr40
                    INTO   lv_c_ext_attr1,lv_c_ext_attr2,lv_c_ext_attr3,lv_c_ext_attr4,lv_c_ext_attr5
                          ,lv_c_ext_attr6,lv_c_ext_attr7,lv_c_ext_attr8,lv_c_ext_attr9,lv_c_ext_attr10
                          ,lv_c_ext_attr11,lv_c_ext_attr12,lv_c_ext_attr13,lv_c_ext_attr14,lv_c_ext_attr15
                          ,lv_c_ext_attr16,lv_c_ext_attr17,lv_c_ext_attr18,lv_c_ext_attr19,lv_c_ext_attr20
                          ,lv_c_ext_attr21,lv_c_ext_attr22,lv_c_ext_attr23,lv_c_ext_attr24,lv_c_ext_attr25
                          ,lv_c_ext_attr26,lv_c_ext_attr27,lv_c_ext_attr28,lv_c_ext_attr29,lv_c_ext_attr30    
                          ,lv_c_ext_attr31,lv_c_ext_attr32,lv_c_ext_attr33,lv_c_ext_attr34,lv_c_ext_attr35
                          ,lv_c_ext_attr36,lv_c_ext_attr37,lv_c_ext_attr38,lv_c_ext_attr39,lv_c_ext_attr40
                          ,ln_n_ext_attr1,ln_n_ext_attr2,ln_n_ext_attr3,ln_n_ext_attr4,ln_n_ext_attr5
                          ,ln_n_ext_attr6,ln_n_ext_attr7,ln_n_ext_attr8,ln_n_ext_attr9,ln_n_ext_attr10    
                          ,ln_n_ext_attr11,ln_n_ext_attr12,ln_n_ext_attr13,ln_n_ext_attr14,ln_n_ext_attr15
                          ,ln_n_ext_attr16,ln_n_ext_attr17,ln_n_ext_attr18,ln_n_ext_attr19,ln_n_ext_attr20    
                          ,lv_uom_ext_attr1,lv_uom_ext_attr2,lv_uom_ext_attr3,lv_uom_ext_attr4,lv_uom_ext_attr5    
                          ,lv_uom_ext_attr6,lv_uom_ext_attr7,lv_uom_ext_attr8,lv_uom_ext_attr9,lv_uom_ext_attr10
                          ,lv_uom_ext_attr11,lv_uom_ext_attr12,lv_uom_ext_attr13,lv_uom_ext_attr14,lv_uom_ext_attr15                          
                          ,lv_uom_ext_attr16,lv_uom_ext_attr17,lv_uom_ext_attr18,lv_uom_ext_attr19,lv_uom_ext_attr20
                          ,ld_d_ext_attr1,ld_d_ext_attr2,ld_d_ext_attr3,ld_d_ext_attr4,ld_d_ext_attr5
                          ,ld_d_ext_attr6,ld_d_ext_attr7,ld_d_ext_attr8,ld_d_ext_attr9,ld_d_ext_attr10
                          ,lv_tl_ext_attr1,lv_tl_ext_attr2,lv_tl_ext_attr3,lv_tl_ext_attr4,lv_tl_ext_attr5
                          ,lv_tl_ext_attr6,lv_tl_ext_attr7,lv_tl_ext_attr8,lv_tl_ext_attr9,lv_tl_ext_attr10
                          ,lv_tl_ext_attr11,lv_tl_ext_attr12,lv_tl_ext_attr13,lv_tl_ext_attr14,lv_tl_ext_attr15
                          ,lv_tl_ext_attr16,lv_tl_ext_attr17,lv_tl_ext_attr18,lv_tl_ext_attr19,lv_tl_ext_attr20
                          ,lv_tl_ext_attr21,lv_tl_ext_attr22,lv_tl_ext_attr23,lv_tl_ext_attr24,lv_tl_ext_attr25
                          ,lv_tl_ext_attr26,lv_tl_ext_attr27,lv_tl_ext_attr28,lv_tl_ext_attr29,lv_tl_ext_attr30
                          ,lv_tl_ext_attr31,lv_tl_ext_attr32,lv_tl_ext_attr33,lv_tl_ext_attr34,lv_tl_ext_attr35
                          ,lv_tl_ext_attr36,lv_tl_ext_attr37,lv_tl_ext_attr38,lv_tl_ext_attr39,lv_tl_ext_attr40
                   FROM apps.ego_mtl_sy_items_ext_vl ego1
                  WHERE ego1.extension_id = l_uda_tab_tbl(index_count).extension_id;
          EXCEPTION
          WHEN OTHERS THEN
           Log_Msg('Exception while UDA values for record with extension_id:' || l_uda_tab_tbl(index_count).extension_id);
          END;
                --Changes for 1.10 End          
                

                IF l_uda_tab_tbl(index_count).data_type_code =  'C' THEN    -- For character data type
                    IF l_uda_tab_tbl(index_count).column_number BETWEEN 1 AND 10 THEN
                        CASE l_uda_tab_tbl(index_count).column_number
                        WHEN 1 THEN v_attr_value:= lv_c_ext_attr1;
                        WHEN 2 THEN v_attr_value:= lv_c_ext_attr2;
                        WHEN 3 THEN v_attr_value:= lv_c_ext_attr3;
                        WHEN 4 THEN v_attr_value:= lv_c_ext_attr4;
                        WHEN 5 THEN v_attr_value:= lv_c_ext_attr5;
                        WHEN 6 THEN v_attr_value:= lv_c_ext_attr6;
                        WHEN 7 THEN v_attr_value:= lv_c_ext_attr7;
                        WHEN 8 THEN v_attr_value:= lv_c_ext_attr8;
                        WHEN 9 THEN v_attr_value:= lv_c_ext_attr9;
                        WHEN 10 THEN v_attr_value:= lv_c_ext_attr10;
                        END CASE;
                    ELSIF l_uda_tab_tbl(index_count).column_number BETWEEN 11 AND 20 THEN
                        CASE l_uda_tab_tbl(index_count).column_number
                            WHEN 11 THEN v_attr_value:= lv_c_ext_attr11;
                            WHEN 12 THEN v_attr_value:= lv_c_ext_attr12;
                            WHEN 13 THEN v_attr_value:= lv_c_ext_attr13;
                            WHEN 14 THEN v_attr_value:= lv_c_ext_attr14;
                            WHEN 15 THEN v_attr_value:= lv_c_ext_attr15;
                            WHEN 16 THEN v_attr_value:= lv_c_ext_attr16;
                            WHEN 17 THEN v_attr_value:= lv_c_ext_attr17;
                            WHEN 18 THEN v_attr_value:= lv_c_ext_attr18;
                            WHEN 19 THEN v_attr_value:= lv_c_ext_attr19;
                            WHEN 20 THEN v_attr_value:= lv_c_ext_attr20;
                        END CASE;
                    ELSIF l_uda_tab_tbl(index_count).column_number BETWEEN 21 AND 30 THEN
                        CASE l_uda_tab_tbl(index_count).column_number
                      WHEN 21 THEN v_attr_value:= lv_c_ext_attr21;
                      WHEN 22 THEN v_attr_value:= lv_c_ext_attr22;
                      WHEN 23 THEN v_attr_value:= lv_c_ext_attr23;
                      WHEN 24 THEN v_attr_value:= lv_c_ext_attr24;
                      WHEN 25 THEN v_attr_value:= lv_c_ext_attr25;
                      WHEN 26 THEN v_attr_value:= lv_c_ext_attr26;
                      WHEN 27 THEN v_attr_value:= lv_c_ext_attr27;
                      WHEN 28 THEN v_attr_value:= lv_c_ext_attr28;
                      WHEN 29 THEN v_attr_value:= lv_c_ext_attr29;
                      WHEN 30 THEN v_attr_value:= lv_c_ext_attr30;
                        END CASE;
                    ELSIF l_uda_tab_tbl(index_count).column_number BETWEEN 31 AND 40 THEN
                        CASE l_uda_tab_tbl(index_count).column_number
                      WHEN 31 THEN v_attr_value:= lv_c_ext_attr31;
                      WHEN 32 THEN v_attr_value:= lv_c_ext_attr32;
                      WHEN 33 THEN v_attr_value:= lv_c_ext_attr33;
                      WHEN 34 THEN v_attr_value:= lv_c_ext_attr34;
                      WHEN 35 THEN v_attr_value:= lv_c_ext_attr35;
                      WHEN 36 THEN v_attr_value:= lv_c_ext_attr36;
                      WHEN 37 THEN v_attr_value:= lv_c_ext_attr37;
                      WHEN 38 THEN v_attr_value:= lv_c_ext_attr38;
                      WHEN 39 THEN v_attr_value:= lv_c_ext_attr39;
                      WHEN 40 THEN v_attr_value:= lv_c_ext_attr40;
                        END CASE;
                    END IF;
                    
                
                ELSIF l_uda_tab_tbl(index_count).data_type_code =  'N' THEN -- For numeric data type
                      IF l_uda_tab_tbl(index_count).column_number BETWEEN 1 AND 5 THEN
                                    CASE l_uda_tab_tbl(index_count).column_number
                          WHEN 1 THEN v_attr_value:= ln_n_ext_attr1;
                          WHEN 2 THEN v_attr_value:= ln_n_ext_attr2;
                          WHEN 3 THEN v_attr_value:= ln_n_ext_attr3;
                          WHEN 4 THEN v_attr_value:= ln_n_ext_attr4;
                          WHEN 5 THEN v_attr_value:= ln_n_ext_attr5;
                                    END CASE;            
                      ELSIF l_uda_tab_tbl(index_count).column_number BETWEEN 6 AND 10 THEN
                                    CASE l_uda_tab_tbl(index_count).column_number
                          WHEN 6 THEN v_attr_value:= ln_n_ext_attr6;
                          WHEN 7 THEN v_attr_value:= ln_n_ext_attr7;
                          WHEN 8 THEN v_attr_value:= ln_n_ext_attr8;
                          WHEN 9 THEN v_attr_value:= ln_n_ext_attr9;
                          WHEN 10 THEN v_attr_value:= ln_n_ext_attr10;
                                    END CASE;            
                      ELSIF l_uda_tab_tbl(index_count).column_number BETWEEN 11 AND 15 THEN
                                    CASE l_uda_tab_tbl(index_count).column_number
                          WHEN 11 THEN v_attr_value:= ln_n_ext_attr11;
                          WHEN 12 THEN v_attr_value:= ln_n_ext_attr12;
                          WHEN 13 THEN v_attr_value:= ln_n_ext_attr13;
                          WHEN 14 THEN v_attr_value:= ln_n_ext_attr14;
                          WHEN 15 THEN v_attr_value:= ln_n_ext_attr15;
                                    END CASE;            
                      ELSIF l_uda_tab_tbl(index_count).column_number BETWEEN 16 AND 20 THEN
                                    CASE l_uda_tab_tbl(index_count).column_number
                          WHEN 16 THEN v_attr_value:= ln_n_ext_attr16;
                          WHEN 17 THEN v_attr_value:= ln_n_ext_attr17;
                          WHEN 18 THEN v_attr_value:= ln_n_ext_attr18;
                          WHEN 19 THEN v_attr_value:= ln_n_ext_attr19;
                          WHEN 20 THEN v_attr_value:= ln_n_ext_attr20;
                                    END CASE;            
                      END IF;
            
                ELSIF l_uda_tab_tbl(index_count).data_type_code =  'A' THEN  -- For Translatable data type    
                    IF l_uda_tab_tbl(index_count).column_number BETWEEN 1 AND 10 THEN
                                  CASE l_uda_tab_tbl(index_count).column_number
                        WHEN 1 THEN v_attr_value:= lv_tl_ext_attr1;
                        WHEN 2 THEN v_attr_value:= lv_tl_ext_attr2;
                        WHEN 3 THEN v_attr_value:= lv_tl_ext_attr3;
                        WHEN 4 THEN v_attr_value:= lv_tl_ext_attr4;
                        WHEN 5 THEN v_attr_value:= lv_tl_ext_attr5;
                        WHEN 6 THEN v_attr_value:= lv_tl_ext_attr6;
                        WHEN 7 THEN v_attr_value:= lv_tl_ext_attr7;
                        WHEN 8 THEN v_attr_value:= lv_tl_ext_attr8;
                        WHEN 9 THEN v_attr_value:= lv_tl_ext_attr9;
                        WHEN 10 THEN v_attr_value:= lv_tl_ext_attr10;
                                  END CASE;            
                    ELSIF l_uda_tab_tbl(index_count).column_number BETWEEN 11 AND 20 THEN
                                  CASE l_uda_tab_tbl(index_count).column_number
                        WHEN 11 THEN v_attr_value:= lv_tl_ext_attr11;
                        WHEN 12 THEN v_attr_value:= lv_tl_ext_attr12;
                        WHEN 13 THEN v_attr_value:= lv_tl_ext_attr13;
                        WHEN 14 THEN v_attr_value:= lv_tl_ext_attr14;
                        WHEN 15 THEN v_attr_value:= lv_tl_ext_attr15;
                        WHEN 16 THEN v_attr_value:= lv_tl_ext_attr16;
                        WHEN 17 THEN v_attr_value:= lv_tl_ext_attr17;
                        WHEN 18 THEN v_attr_value:= lv_tl_ext_attr18;
                        WHEN 19 THEN v_attr_value:= lv_tl_ext_attr19;
                        WHEN 20 THEN v_attr_value:= lv_tl_ext_attr20;
                                  END CASE;                
                    ELSIF l_uda_tab_tbl(index_count).column_number BETWEEN 21 AND 30 THEN
                                  CASE l_uda_tab_tbl(index_count).column_number
                        WHEN 21 THEN v_attr_value:= lv_tl_ext_attr21;
                        WHEN 22 THEN v_attr_value:= lv_tl_ext_attr22;
                        WHEN 23 THEN v_attr_value:= lv_tl_ext_attr23;
                        WHEN 24 THEN v_attr_value:= lv_tl_ext_attr24;
                        WHEN 25 THEN v_attr_value:= lv_tl_ext_attr25;
                        WHEN 26 THEN v_attr_value:= lv_tl_ext_attr26;
                        WHEN 27 THEN v_attr_value:= lv_tl_ext_attr27;
                        WHEN 28 THEN v_attr_value:= lv_tl_ext_attr28;
                        WHEN 29 THEN v_attr_value:= lv_tl_ext_attr29;
                        WHEN 30 THEN v_attr_value:= lv_tl_ext_attr30;
                                  END CASE;            
                    ELSIF l_uda_tab_tbl(index_count).column_number BETWEEN 31 AND 40 THEN
                                  CASE l_uda_tab_tbl(index_count).column_number
                        WHEN 31 THEN v_attr_value:= lv_tl_ext_attr31;
                        WHEN 32 THEN v_attr_value:= lv_tl_ext_attr32;
                        WHEN 33 THEN v_attr_value:= lv_tl_ext_attr33;
                        WHEN 34 THEN v_attr_value:= lv_tl_ext_attr34;
                        WHEN 35 THEN v_attr_value:= lv_tl_ext_attr35;
                        WHEN 36 THEN v_attr_value:= lv_tl_ext_attr36;
                        WHEN 37 THEN v_attr_value:= lv_tl_ext_attr37;
                        WHEN 38 THEN v_attr_value:= lv_tl_ext_attr38;
                        WHEN 39 THEN v_attr_value:= lv_tl_ext_attr39;
                        WHEN 40 THEN v_attr_value:= lv_tl_ext_attr40;
                                  END CASE;            
                    END IF;
                
                ELSIF l_uda_tab_tbl(index_count).data_type_code =  'X' THEN  -- For Date data type
                    IF l_uda_tab_tbl(index_count).column_number BETWEEN 1 AND 5 THEN
                                  CASE l_uda_tab_tbl(index_count).column_number
                        WHEN 1 THEN v_attr_value:= to_char(ld_d_ext_attr1, 'DD-MM-YYYY HH24:MI:SS');
                        WHEN 2 THEN v_attr_value:= to_char(ld_d_ext_attr2, 'DD-MM-YYYY HH24:MI:SS');
                        WHEN 3 THEN v_attr_value:= to_char(ld_d_ext_attr3, 'DD-MM-YYYY HH24:MI:SS');
                        WHEN 4 THEN v_attr_value:= to_char(ld_d_ext_attr4, 'DD-MM-YYYY HH24:MI:SS');
                        WHEN 5 THEN v_attr_value:= to_char(ld_d_ext_attr5, 'DD-MM-YYYY HH24:MI:SS');
                                  END CASE;            
                    ELSIF l_uda_tab_tbl(index_count).column_number BETWEEN 6 AND 10 THEN
                                  CASE l_uda_tab_tbl(index_count).column_number
                        WHEN 6 THEN v_attr_value:= to_char(ld_d_ext_attr6, 'DD-MM-YYYY HH24:MI:SS');
                        WHEN 7 THEN v_attr_value:= to_char(ld_d_ext_attr7, 'DD-MM-YYYY HH24:MI:SS');
                        WHEN 8 THEN v_attr_value:= to_char(ld_d_ext_attr8, 'DD-MM-YYYY HH24:MI:SS');
                        WHEN 9 THEN v_attr_value:= to_char(ld_d_ext_attr9, 'DD-MM-YYYY HH24:MI:SS');
                        WHEN 10 THEN v_attr_value:= to_char(ld_d_ext_attr10, 'DD-MM-YYYY HH24:MI:SS');
                                  END CASE;            
                    END IF;
                ELSE    -- For UOM data type
                    IF l_uda_tab_tbl(index_count).column_number BETWEEN 1 AND 5 THEN
                                  CASE l_uda_tab_tbl(index_count).column_number
                        WHEN 1 THEN v_attr_value:= lv_uom_ext_attr1;
                        WHEN 2 THEN v_attr_value:= lv_uom_ext_attr2;
                        WHEN 3 THEN v_attr_value:= lv_uom_ext_attr3;
                        WHEN 4 THEN v_attr_value:= lv_uom_ext_attr4;
                        WHEN 5 THEN v_attr_value:= lv_uom_ext_attr5;
                                  END CASE;            
                    ELSIF l_uda_tab_tbl(index_count).column_number BETWEEN 6 AND 10 THEN
                                  CASE l_uda_tab_tbl(index_count).column_number
                        WHEN 6 THEN v_attr_value:= lv_uom_ext_attr6;
                        WHEN 7 THEN v_attr_value:= lv_uom_ext_attr7;
                        WHEN 8 THEN v_attr_value:= lv_uom_ext_attr8;
                        WHEN 9 THEN v_attr_value:= lv_uom_ext_attr9;
                        WHEN 10 THEN v_attr_value:= lv_uom_ext_attr10;
                                  END CASE;            
                    ELSIF l_uda_tab_tbl(index_count).column_number BETWEEN 11 AND 15 THEN
                                  CASE l_uda_tab_tbl(index_count).column_number
                        WHEN 11 THEN v_attr_value:= lv_uom_ext_attr11;
                        WHEN 12 THEN v_attr_value:= lv_uom_ext_attr12;
                        WHEN 13 THEN v_attr_value:= lv_uom_ext_attr13;
                        WHEN 14 THEN v_attr_value:= lv_uom_ext_attr14;
                        WHEN 15 THEN v_attr_value:= lv_uom_ext_attr15;
                                  END CASE;            
                    ELSIF l_uda_tab_tbl(index_count).column_number BETWEEN 16 AND 20 THEN    
                                  CASE l_uda_tab_tbl(index_count).column_number
                        WHEN 16 THEN v_attr_value:= lv_uom_ext_attr16;
                        WHEN 17 THEN v_attr_value:= lv_uom_ext_attr17;
                        WHEN 18 THEN v_attr_value:= lv_uom_ext_attr18;
                        WHEN 19 THEN v_attr_value:= lv_uom_ext_attr19;
                        WHEN 20 THEN v_attr_value:= lv_uom_ext_attr20;
                                  END CASE;                
                    END IF;
                END IF;  

                -- We will not publish value set name in UDA file for seeded attributes
                IF  l_uda_tab_tbl(index_count).attr_group_name NOT LIKE 'PSO%' THEN
                    l_uda_tab_tbl(index_count).flex_value_set_name := NULL;
                END IF;

                IF v_attr_value IS NOT NULL
                THEN 
                    l_rec_count     := l_rec_count + 1;
                    
                    UTL_FILE.put_line(  l_file,
                            l_uda_tab_tbl(index_count).inventory_item_id
                        ||','||   replace( l_uda_tab_tbl(index_count).attr_group_name    ,',','~,')       
                        ||','||   replace( l_uda_tab_tbl(index_count).attr_group_disp_name  ,',','~,')    
                        ||','||   replace( l_uda_tab_tbl(index_count).multi_row_code  ,',','~,')          
                        ||','||   replace( l_uda_tab_tbl(index_count).Group_Identifier ,',','~,')         
                        ||','||   replace( l_uda_tab_tbl(index_count).attr_name  ,',','~,')               
                        ||','||   replace( l_uda_tab_tbl(index_count).attr_display_name ,',','~,')        
                        ||','||   replace( l_uda_tab_tbl(index_count).data_type_code ,',','~,')           
                        ||','||   replace( v_attr_value  ,',','~,')              
                        ||','||   replace( l_uda_tab_tbl(index_count).flex_value_set_name  ,',','~,')     
                        ||','||   replace( to_char(l_uda_tab_tbl(index_count).creation_date   , 'DD-MM-YYYY HH24:MI:SS')   ,',','~,')         
                        ||','||   replace( l_uda_tab_tbl(index_count).created_by_name ,',','~,')          
                        ||','||   replace( to_char(l_uda_tab_tbl(index_count).last_update_date , 'DD-MM-YYYY HH24:MI:SS') ,',','~,')          
                        ||','||   replace( l_uda_tab_tbl(index_count).last_updated_by_name ,',','~,')      
                        ,TRUE --Changes for v1.8
                             );   
                             
                    l_suc_count := l_suc_count + 1;
                END IF;
          EXCEPTION
          WHEN OTHERS THEN
                    Log_Msg('Unable to write to file for Item UDA for Inventory Item Id:'||l_uda_tab_tbl(index_count).inventory_item_id
                    ||' -Error Message:' || SQLERRM);         
          END;
        END LOOP;
      END LOOP;
      CLOSE c_uda;
        
        IF l_rec_count = l_suc_count
        THEN
            Log_Msg('Item UDA File generated successfully');
            
            -- Printing the end of file line  
            UTL_FILE.put_line(l_file, 'Total No of Records - ' ||','|| l_suc_count ); 
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Success');
        ELSE    
            Log_Msg('Error while generating Item UDA File');
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Failed');
        END IF; 
        
        UTL_FILE.fclose (l_file);
        
        Log_Msg('UDA File generated successfully');
        Log_Msg(' ');
        Log_Msg('End of generate_uda_file Procedure');
        Log_Msg(gcn_print_line); 
        
        -- Writing to Output File 
        Out_Msg(gcn_print_line); 
        Out_Msg('No of records pulled from database for Item UDA           : '|| l_rec_count);
        Out_Msg('No of records inserted into Item UDA CSV File             : '|| l_suc_count);
        Out_Msg(gcn_print_line); 
        Out_Msg(' ');        
        
    EXCEPTION
       WHEN OTHERS
       THEN
          Log_Msg('Error in generate_uda_file procedure -' || SQLERRM);
    
    END generate_uda_file;
    
    
    

/* ********************************************************
   * Procedure: generate_xref_file
   *
   * Synopsis: This procedure is to generate XREF datafiles at the given path
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
   * Narendra Mishra    1.0                                            24-Jun-2016
   ************************************************************************************* */   
    PROCEDURE generate_xref_file
    IS
        l_file          UTL_FILE.file_type;
        l_file_name     VARCHAR2(100) := 'XXPSO_CROSS_REF_DATA';
        l_rec_count     NUMBER              := 0;
        l_suc_count     NUMBER              := 0;          
        v_attr_value    VARCHAR2 (2000);
        
        CURSOR c_xref
        IS
              SELECT mcr.inventory_item_id              inventory_item_id
                    ,mcr.cross_reference_type           cross_reference_type
                    ,mcr.cross_reference                cross_reference
                    ,mcr.description                    description
                    ,hosv.orig_system                   orig_system
                    ,fu1.user_name                      created_by_name
                    ,mcr.creation_date                  creation_date        
                    ,fu2.user_name                      last_updated_by_name
                    ,mcr.last_update_date               last_update_date
               FROM mtl_cross_references                mcr
                    ,hz_orig_systems_vl                  hosv
                    ,apps.fnd_user                       fu1
                    ,apps.fnd_user                       fu2
                    ,apps.xxpso_ego_item_change_gtt    item_gtt
                    --      ,apps.mtl_parameters        mp
              WHERE mcr.source_system_id            = hosv.orig_system_id
                AND mcr.cross_reference_type        = 'SS_ITEM_XREF'
                AND TRUNC(SYSDATE)  BETWEEN TRUNC(NVL(mcr.start_date_active,SYSDATE-1)) 
                    AND TRUNC(NVL(mcr.end_date_active,SYSDATE+1))
                AND TRUNC(SYSDATE)  BETWEEN TRUNC(NVL(hosv.start_date_active,SYSDATE-1)) 
                    AND TRUNC(NVL(hosv.end_date_active,SYSDATE+1))
                AND hosv.status                     = 'A'
                AND mcr.created_by                  = fu1.user_id 
                AND mcr.last_updated_by             = fu2.user_id 
                --Changes for v1.5
                AND mcr.inventory_item_id         = item_gtt.inventory_item_id  
                 -- AND mcr.organization_id         = mp.organization_id
               -- AND mp.organization_code        = 'MDM'
                /*
                AND mcr.inventory_item_id IN 
                 (SELECT inventory_item_id
                    FROM apps.mtl_system_items_b
                   WHERE last_update_date BETWEEN g_start_date AND g_end_date
                   UNION
                  SELECT inventory_item_id 
                    FROM apps.ego_mtl_sy_items_ext_b 
                   WHERE last_update_date BETWEEN g_start_date AND g_end_date
                      )*/
           ORDER BY mcr.inventory_item_id,hosv.orig_system;
                  
    BEGIN
    
        Log_Msg(gcn_print_line);
        Log_Msg('Start of generate_xref_file Procedure');
    
        l_file_name := l_file_name||'_'||TO_CHAR(gcn_request_id)||'_'||g_sysdate||'.csv';
        
        Log_Msg('DBA Directory Name  - ' || gc_dba_directory_name);
        Log_Msg('File Name  - ' || l_file_name);
        
        --Changes for version 1.8 start
        l_file := UTL_FILE.fopen (gc_dba_directory_name, l_file_name, 'w',32767);
    --Changes for version 1.8 end
        
        UTL_FILE.put_line(  l_file,
                       'ItemId'                               
                ||','||    'CrossReferenceType'      
                ||','||    'CrossReference'                    
                ||','||    'Description'             
                ||','||    'SourceSystem'            
                ||','||    'CreationDate'            
                ||','||    'CreatedBy'               
                ||','||    'LastUpdateDate'          
                ||','||    'LastUpdatedBy'         
                ,TRUE --Changes for v1.8
                         );
        
        FOR c IN c_xref
        LOOP
            l_rec_count := l_rec_count + 1;
            
            BEGIN        
            UTL_FILE.put_line(  l_file,
                                    c.inventory_item_id
                        ||','||  replace(  c.cross_reference_type ,',','~,')     
                        ||','||  replace(  c.cross_reference ,',','~,')          
                        ||','||  replace(  c.description ,',','~,')              
                        ||','||  replace(  c.orig_system ,',','~,')              
                        ||','||  replace(  to_char(c.creation_date  , 'DD-MM-YYYY HH24:MI:SS')  ,',','~,')           
                        ||','||  replace(  c.created_by_name ,',','~,')          
                        ||','||  replace(  to_char(c.last_update_date  , 'DD-MM-YYYY HH24:MI:SS'),',','~,')          
                        ||','||  replace(  c.last_updated_by_name ,',','~,')      
                        ,TRUE --Changes for v1.8
                             );   
                                                             
            l_suc_count := l_suc_count + 1;
        EXCEPTION
            WHEN OTHERS
            THEN
            Log_Msg('Unable to write to file for Item XRef for Inventory Item Id:'||c.inventory_item_id
            ||' -Error Message:' || SQLERRM);
            END; 
        END LOOP;
        
        IF l_rec_count = l_suc_count
        THEN
            Log_Msg('Item XRef File generated successfully');
            
            -- Printing the end of file line  
            UTL_FILE.put_line(l_file, 'Total No of Records - ' ||','|| l_suc_count );  
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Success');
        ELSE    
            Log_Msg('Error while generating Item XRef File');
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Failed');
        END IF; 
        
        UTL_FILE.fclose (l_file);
        
        Log_Msg('XREF File generated successfully');
        Log_Msg(' ');
        Log_Msg('End of generate_xref_file Procedure');
        Log_Msg(gcn_print_line); 

        -- Writing to Output File 
        Out_Msg(gcn_print_line); 
        Out_Msg('No of records pulled from database for Item XRef           : '|| l_rec_count);
        Out_Msg('No of records inserted into Item XRef CSV File             : '|| l_suc_count);
        Out_Msg(gcn_print_line); 
        Out_Msg(' ');   
        
    EXCEPTION
       WHEN OTHERS
       THEN
          Log_Msg('Error in generate_xref_file procedure -' || SQLERRM);
    
    END generate_xref_file;
    
    

/* ********************************************************
   * Procedure: generate_relationship_file
   *
   * Synopsis: This procedure is to generate item relationship datafiles at the given path
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
   * Narendra Mishra    1.0                                            23-Jun-2016
   ************************************************************************************* */   
    PROCEDURE generate_relationship_file
    IS
        l_file                  UTL_FILE.file_type;
        l_file_name             VARCHAR2(100) := 'XXPSO_ITEM_RELATIONSHIP_DATA';
        l_rec_count             NUMBER              := 0;
        l_suc_count             NUMBER              := 0;           
        lv_file_line            VARCHAR2(25000);
        lv_file_multi_line      VARCHAR2(30000);
    
    CURSOR cur_relationship 
    IS
          SELECT rel.inventory_item_id              inventory_item_id
                --,rel.organization_id                organization_id
                --,item.segment1                      item_segment1
                ,rel.related_item_id                related_item_id
                ,rel_item.segment1                  rel_item_segment1
                ,rel.organization_id            organization_id
                ,lkp.meaning                        relationship_type
                ,rel.relationship_type_id           relationship_type_id
                ,rel.inventory_item_id || '_' ||
                 rel.related_item_id||'_' ||
                 rel.relationship_type_id||'_'||
                 rel.organization_id                mdm_reference_id                    
                ,rel.reciprocal_flag                reciprocal_flag
                ,rel.planning_enabled_flag          planning_enabled_flag
                ,dff.attr_char1                     attr_char1
                ,dff.attr_char2                     attr_char2
                ,dff.attr_char3                     attr_char3
                --,rel.start_date                     start_date
                --,rel.end_date                       end_date
                --,TO_DATE(rel.ATTR_DATE1,'YYYY/MM/DD HH24:MI:SS')    start_date -- changes for 1.12 
                --,TO_DATE(rel.ATTR_DATE2,'YYYY/MM/DD HH24:MI:SS')    end_date -- changes for 1.12 
                ,dff.start_date start_date -- changes for 1.12 
                ,dff.end_date end_date -- changes for 1.12 
                ,fu1.user_name                      created_by_name
                ,NVL(dff.creation_date,rel.creation_date) creation_date     -- changes for 1.13   -- changes for 1.15
                ,fu2.user_name                      last_updated_by_name
                ,NVL(dff.last_update_date,rel.last_update_date)   last_update_date -- changes for 1.13 -- changes for 1.15
           FROM apps.mtl_related_items          rel,
                apps.xxpso_ego_related_itms_dff     dff,
                --mtl_system_items_b            item,
                apps.mtl_system_items_b         rel_item,
                apps.mfg_lookups                lkp,
                apps.fnd_user                   fu1,
                apps.fnd_user                   fu2
          WHERE 
            --AND rel.inventory_item_id   = item.inventory_item_id
            --AND rel.organization_id     = item.organization_id
            rel.related_item_id             = rel_item.inventory_item_id -- changes for 1.12 
            AND rel.organization_id             = rel_item.organization_id
            AND rel.inventory_item_id             = dff.inventory_item_id(+)
            AND rel.organization_id             = dff.organization_id(+)
            AND rel.related_item_id             = dff.related_item_id(+)
            AND rel.relationship_type_id        = dff.relation_type(+)
            AND lkp.lookup_type                 = 'MTL_RELATIONSHIP_TYPES'
            AND lkp.enabled_flag                = 'Y'
            AND SYSDATE BETWEEN                 NVL(lkp.start_date_active,SYSDATE) AND NVL(lkp.end_date_active,SYSDATE+1)
            AND lkp.lookup_code                 = TO_CHAR(rel.relationship_type_id)
            AND rel.created_by                  = fu1.user_id 
            AND rel.last_updated_by             = fu2.user_id 
            AND NVL(dff.last_update_date,rel.last_update_date) BETWEEN g_start_date AND g_end_date -- changes for 1.13 -- changes for 1.15
            --AND ( 
            --         (  (rel.end_date IS NULL or rel.end_date > g_end_date) 
            --         AND NVL(rel.start_date,'01-JAN-1990') <= g_end_date 
            --         )
            --      OR rel.end_date BETWEEN g_start_date AND g_end_date
              --    )
            --AND ( 
            --        TO_DATE(rel.ATTR_DATE2,'YYYY/MM/DD HH24:MI:SS') IS NULL
            --        OR TO_DATE(rel.ATTR_DATE2,'YYYY/MM/DD HH24:MI:SS')> SYSDATE                       
            --    )
            AND ( --(rel.end_date IS NULL  OR rel.end_date > SYSDATE )  -- changes for 1.13
            	  --AND -- changes for 1.13
                (dff.end_date IS NULL  OR dff.end_date > SYSDATE )      
            	)
       ORDER BY rel.inventory_item_id, rel.related_item_id, rel.relationship_type_id;

       l_rec_related_items_rec        xxpso_ego_related_itms_stg%ROWTYPE;
       lv_rel_item_record_found        VARCHAR2(1);
       lv_originating_system        VARCHAR2(10);
       lv_originating_system_ref     VARCHAR2(255);  
       lv_originating_system_rel_ref     VARCHAR2(255);
       lv_originating_system_rel_typ    NUMBER;
       lv_originating_system_date     DATE;  
       lv_transaction_type        VARCHAR2(10);
       
       --Changes for v1.11 Begin
       lv_attrchar1_vsname		VARCHAR2(100);
       lv_attrchar2_vsname		VARCHAR2(100);
       lv_attrchar3_vsname		VARCHAR2(100);
       lv_char1_vsname			VARCHAR2(100);
       lv_char2_vsname			VARCHAR2(100);
       lv_char3_vsname			VARCHAR2(100);       
       --Changes for v1.11 End
       
    BEGIN
    
        Log_Msg(gcn_print_line);
        Log_Msg('Start of generate_relationship_file Procedure');
    
        l_file_name := l_file_name||'_'||TO_CHAR(gcn_request_id)||'_'||g_sysdate||'.csv';
        
        Log_Msg('DBA Directory name  - ' || gc_dba_directory_name);
        Log_Msg('File Name  - ' || l_file_name);
        
        --Changes for version 1.8 start
        l_file := UTL_FILE.fopen (gc_dba_directory_name, l_file_name, 'w',32767);
    --Changes for version 1.8 end
	
        UTL_FILE.put_line(  l_file,
            --Header related parameters
                            'OrigSystem'            
                ||','||    'OrigSystemReference'   
                ||','||    'OrigSystemTimeStamp'   
                ||','||    'MDMReference'          
                ||','||    'RecordType'            
                ||','||    'Entity'              
                -- Entity specific parameters
                ||','||    'ItemId'                        
                ||','||    'RelatedItemId'               
                ||','||    'RelatedItemNumber'           
                ||','||    'RelationshipType'            
                ||','||    'ReciprocalFlag'              
                ||','||    'PlanningEnabledFlag'        
                ||','||    'AttrChar1'    
                ||','||    'AttrChar1ValueSetName' --Changes for v1.11
                ||','||    'AttrChar2'            
                ||','||    'AttrChar2ValueSetName' --Changes for v1.11
                ||','||    'AttrChar3'                    
                ||','||    'AttrChar3ValueSetName' --Changes for v1.11
                ||','||    'StartDate'                   
                ||','||    'EndDate'                     
                ||','||    'CreationDate'                
                ||','||    'CreatedBy'                   
                ||','||    'LastUpdateDate'              
                ||','||    'LastUpdatedBy'              
                 ,TRUE --Changes for v1.8
                         );  
        
	 --Changes for v1.11 Begin
	 lv_attrchar1_vsname := get_rel_valueset_name('ATTR_CHAR1');
	 lv_attrchar2_vsname := get_rel_valueset_name('ATTR_CHAR2');
	 lv_attrchar3_vsname := get_rel_valueset_name('ATTR_CHAR3');
	 --Changes for v1.11 End
	  Log_Msg('Value set name retrived. lv_attrchar1_vsname:'||lv_attrchar1_vsname||
	  	  ' lv_attrchar2_vsname:'||lv_attrchar2_vsname||' lv_attrchar3_vsname:'||lv_attrchar3_vsname);
        FOR c IN cur_relationship
        LOOP
        
            l_rec_count := l_rec_count + 1;
            lv_rel_item_record_found := 'N';
                --Get the last updated record.
                BEGIN
        Log_Msg('Fetching Integration Related item information for inventory_item_id:'||c.inventory_item_id||
                ' related_item_id:'||c.related_item_id ||
                ' relationship_type_id:'||c.relationship_type_id ||
                ' organization_id:'||c.organization_id
                );                
        SELECT * 
          INTO l_rec_related_items_rec
          FROM xxpso_ego_related_itms_stg stg1
         WHERE stg1.inventory_item_id = c.inventory_item_id
           AND stg1.related_item_id = c.related_item_id
                   AND stg1.organization_id = c.organization_id
                   AND stg1.relation_type   = c.relationship_type_id
                   AND stg1.status_code = gv_processed_flag
                   AND stg1.published_by_etl = 'N'
                   AND stg1.last_update_date = (select max(stg2.last_update_date)
                                    FROM  xxpso_ego_related_itms_stg stg2
                                    WHERE stg2.inventory_item_id = stg1.inventory_item_id
                                      AND stg2.related_item_id = stg1.related_item_id
                          AND stg2.organization_id = stg1.organization_id
                          AND stg2.relation_type = stg1.relation_type
                          AND stg2.status_code = gv_processed_flag
                          AND stg2.published_by_etl = 'N'                          
                       );
        

                
                Log_Msg('Relation record found: '||l_rec_related_items_rec.record_id);
                lv_rel_item_record_found := 'Y';
                
                EXCEPTION
                WHEN OTHERS THEN
                lv_rel_item_record_found := 'N';
                END;
                
                IF lv_rel_item_record_found = 'Y' THEN
                    lv_originating_system := l_rec_related_items_rec.source_system;
                    lv_originating_system_ref := l_rec_related_items_rec.source_item_number;
                    lv_originating_system_date := l_rec_related_items_rec.last_update_date;
                    IF l_rec_related_items_rec.transaction_type = 'CREATE' THEN
                    	lv_transaction_type       := 'C';
                    ELSIF l_rec_related_items_rec.transaction_type = 'UPDATE' THEN
                    	lv_transaction_type       := 'U';
                    END IF;
                    --lv_transaction_type       := l_rec_related_items_rec.transaction_type;
                    Log_Msg('Record Id of matched Related Item:'||l_rec_related_items_rec.record_id);   
                ELSE
                    lv_originating_system := 'MDM';
                    lv_originating_system_date := SYSDATE;
                    lv_originating_system_ref  := NULL;--Changes for v1.14
                    lv_transaction_type	       := NULL;--Changes for v1.14                    
                END IF;          
            BEGIN  
            
            --Changes for v1.11 Begin
            IF c.attr_char1 IS NULL THEN
            lv_char1_vsname := NULL;
            ELSE
            lv_char1_vsname := lv_attrchar1_vsname;
            END IF;
            
            IF c.attr_char2 IS NULL THEN
            lv_char2_vsname := NULL;
            ELSE
            lv_char2_vsname := lv_attrchar2_vsname;
            END IF;
            
            IF c.attr_char3 IS NULL THEN
            lv_char3_vsname := NULL;
            ELSE
            lv_char3_vsname := lv_attrchar3_vsname;
            END IF;            
            
            
            --Changes for v1.11 End
            
            lv_file_line :=         
            -- Header specific parameters
                replace(lv_originating_system ,',','~,')         
            ||','||   replace( lv_originating_system_ref ,',','~,')  
            ||','||   replace( to_char(lv_originating_system_date,'DD-MM-YYYY HH24:MI:SS')  ,',','~,')
            ||','||   replace( c.mdm_reference_id    ,',','~,')    
            ||','||   replace( lv_transaction_type  ,',','~,')       
            ||','||   replace( gv_related_item_entity ,',','~,')        
            -- Entity specific parameters
            ||','||      replace( c.inventory_item_id ,',','~,')      
            ||','||   replace( c.related_item_id   ,',','~,')   
            ||','||   replace( c.rel_item_segment1 ,',','~,')         
            ||','||   replace( c.relationship_type ,',','~,')        
            ||','||   replace( c.reciprocal_flag   ,',','~,')        
            ||','||   replace( c.planning_enabled_flag ,',','~,')    
            ||','||   replace( c.attr_char1 ,',','~,')  
            ||','||   replace( lv_char1_vsname ,',','~,')  --Changes for v1.11 					
            ||','||   replace( c.attr_char2 ,',','~,')
            ||','||   replace( lv_char2_vsname ,',','~,')  --Changes for v1.11 
            ||','||   replace( c.attr_char3 ,',','~,') 
            ||','||   replace( lv_char3_vsname ,',','~,')  --Changes for v1.11 
            ||','||   replace( to_char(c.start_date , 'DD-MM-YYYY HH24:MI:SS') ,',','~,')                 
            ||','||   replace( to_char(c.end_date , 'DD-MM-YYYY HH24:MI:SS') ,',','~,')                  
            ||','||   replace( to_char(c.creation_date , 'DD-MM-YYYY HH24:MI:SS'),',','~,')              
            ||','||   replace( c.created_by_name ,',','~,')          
            ||','||   replace( to_char(c.last_update_date , 'DD-MM-YYYY HH24:MI:SS') ,',','~,')          
            ||','||   replace( c.last_updated_by_name ,',','~,')      ;                               
            
            UTL_FILE.put_line(l_file,lv_file_line,TRUE );--Changes for v1.8;            
                                                             
            l_suc_count := l_suc_count + 1;
        --After getting the last updated record update     published_by_etl flag to Y for all earlier and exising records.           
        UPDATE xxpso_ego_related_itms_stg
           SET published_by_etl = 'Y'
         WHERE inventory_item_id = c.inventory_item_id
           AND related_item_id = c.related_item_id
           AND organization_id = c.organization_id
           AND relation_type = c.relationship_type_id
                   AND status_code = gv_processed_flag
                   AND published_by_etl = 'N';            
        EXCEPTION
            WHEN OTHERS
            THEN
            Log_Msg('Unable to write to file for Item Relationship: Item Id:'||c.inventory_item_id
            ||' Related Item Id:'||c.related_item_id 
            ||' Relationship Type :'||c.relationship_type 
            ||' -Error Message:' || SQLERRM);
            END;             
            
        END LOOP;
        
        IF l_rec_count = l_suc_count
        THEN
            Log_Msg('Item Relationship File generated successfully');
            
            -- Printing the end of file line  
            UTL_FILE.put_line(l_file, 'Total No of Records - ' ||','|| l_suc_count );  
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Success');
        ELSE    
            Log_Msg('Error while generating Item Relationship File');
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Failed');
        END IF; 
        
        
        UTL_FILE.fclose (l_file);
        
        Log_Msg('Related Item File generated successfully');
        Log_Msg(' ');
        Log_Msg('End of generate_relationship_file Procedure');
        Log_Msg(gcn_print_line); 
        
        -- Writing to Output File 
        Out_Msg(gcn_print_line); 
        Out_Msg('No of records pulled from database for Item Relationship           : '|| l_rec_count);
        Out_Msg('No of records inserted into Item Relationship CSV File             : '|| l_suc_count);
        Out_Msg(gcn_print_line); 
        Out_Msg(' ');           
        
    EXCEPTION
       WHEN OTHERS
       THEN
          Log_Msg('Error in generate_relationship_file procedure -' || SQLERRM);
    
    END generate_relationship_file;

/* ********************************************************
   * Procedure: generate_bom_file
   *
   * Synopsis: This procedure is to generate item bom datafiles at the given path
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
   * Narendra Mishra    1.0                                            23-Jun-2016
   * Akshay Nayak         | 2-Sep-2016    | 1.4      | Send valid BOM and Related Items.       
   ************************************************************************************* */   
    PROCEDURE generate_bom_file
    IS
        l_file                  UTL_FILE.file_type;
        l_file_name             VARCHAR2(100) := 'XXPSO_ITEM_BOM';
        l_rec_count             NUMBER              := 0;
        l_suc_count             NUMBER              := 0;           
        lv_file_line            VARCHAR2(2500);
        lv_file_multi_line      VARCHAR2(30000);
    
    CURSOR cur_item_bom
    IS
         SELECT bom.assembly_item_id            assembly_item_id,
                bom_item.segment1               bom_segment1,
                bom.alternate_bom_designator    structure_name,
                bom.organization_id        organization_id,
                bom.bill_sequence_id        bill_sequence_id,
                bst.structure_type_name         structure_type,
                org.organization_code           organization_code,
                (select name from hr_all_organization_units 
                  where organization_id = org.organization_id) organization_name,
                DECODE(bom.assembly_type, 1, 'Manufacturing Bill', 'Engineering Bill') assembly_type,
                bic.component_item_id           component_item_id,
                comp_item.segment1              comp_segment1,
                bic.component_quantity,
                bic.operation_seq_num,
                bic.item_num,
                bic.planning_factor,                
                bic.effectivity_date,
                bic.disable_date
           FROM apps.bom_bill_of_materials          bom,
                apps.bom_inventory_components       bic,
                apps.mtl_system_items_b             bom_item,
                apps.mtl_system_items_b             comp_item,
                apps.bom_structure_types_b          bst,
                apps.mtl_parameters                 org
          WHERE bom.source_bill_sequence_id     = bic.bill_sequence_id
            AND bom_item.inventory_item_id      = bom.assembly_item_id
            AND bom_item.organization_id        = bom.organization_id
            AND bom_item.organization_id        = org.organization_id
            AND comp_item.inventory_item_id     = bic.component_item_id
            AND comp_item.organization_id       = bom.organization_id 
            AND bom.structure_type_id           = bst.structure_type_id
            --Changes for v1.4 Begin
            -- Below condition will fetch all the component items in a BOM if any component item changes.
            AND bom.bill_sequence_id IN (
                                SELECT bom1.bill_sequence_id 
                                  FROM bom_bill_of_materials bom1 
                                 WHERE bom1.last_update_date          BETWEEN g_start_date AND g_end_date
                                 UNION
                                SELECT bic1.bill_sequence_id 
                                  FROM bom_inventory_components bic1 
                                 WHERE bic1.last_update_date          BETWEEN g_start_date AND g_end_date                                
                            )
        --Below condition will ignore end dates components
        -- disable_date of component if it is NULL it means it is valid.
        -- if disable_date is not null then it should be valid for given time period.
        AND ( bic.disable_date IS NULL or bic.disable_date > g_end_date )
            --AND ( bom.last_update_date          BETWEEN g_start_date AND g_end_date
            --    OR bic.last_update_date         BETWEEN g_start_date AND g_end_date
            --    )
            --Changes for v1.4 End
       ORDER BY bom.assembly_item_id, bic.component_item_id;
       
       l_rec_bom_assembly_rec        xxpso_ego_bom_assembly_stg%ROWTYPE;
       lv_bom_record_found        VARCHAR2(1);
       lv_originating_system        VARCHAR2(10);
       lv_originating_system_ref     VARCHAR2(255);  
       lv_originating_system_date     DATE;  
       lv_transaction_type        VARCHAR2(10);
        
    BEGIN
    
        Log_Msg(gcn_print_line);
        Log_Msg('Start of generate_bom_file Procedure');
    
        l_file_name := l_file_name||'_'||TO_CHAR(gcn_request_id)||'_'||g_sysdate||'.csv';
        
        Log_Msg('DBA Directory name  - ' || gc_dba_directory_name);
        Log_Msg('File Name  - ' || l_file_name);
        
        --Changes for version 1.8 start
        l_file := UTL_FILE.fopen (gc_dba_directory_name, l_file_name, 'w',32767);
    --Changes for version 1.8 end
        
        UTL_FILE.put_line(  l_file,
            --Header related parameters
                       'OrigSystem'            
            ||','||    'OrigSystemReference'   
            ||','||    'OrigSystemTimeStamp'   
            ||','||    'MDMReference'          
            ||','||    'RecordType'            
            ||','||    'Entity'              
            -- Entity specific parameters
            ||','||       'AssemblyItemId'                          
            ||','||    'AssemblyItemNumber'                    
            ||','||    'StructureName'                   
            ||','||    'StructureType'                   
            ||','||    'OrganizationCode'                
            ||','||    'OrganizationName'                
            ||','||    'AssemblyType'                    
            ||','||    'ComponentItemId'                 
            ||','||    'ComponentItemNumber'             
            ||','||    'ComponentQuantity'               
            ||','||    'OperationSequenceNumber'         
            ||','||    'ItemSequenceNumber'              
            ||','||    'PlanningFactor'                  
            ||','||    'FromDate'                        
            ||','||    'ToDate'            
            ,TRUE --Changes for v1.8
            );
        
        FOR c IN cur_item_bom
        LOOP
            l_rec_count := l_rec_count + 1;
            lv_bom_record_found := 'N';
            BEGIN  
                --Get the last updated record.
                BEGIN
        Log_Msg('Fetching Integration BOM item information for assembly_item_id:'||c.assembly_item_id||
                ' structure_name:'||c.structure_name ||
                ' organization_id:'||c.organization_id);                
        SELECT * 
          INTO l_rec_bom_assembly_rec
          FROM xxpso_ego_bom_assembly_stg stg1
         WHERE assembly_item_id = c.assembly_item_id
           AND organization_id = c.organization_id
                   AND structure_name = NVL(c.structure_name,'Primary')
                   AND status_code = gv_processed_flag
                   AND published_by_etl = 'N'
                   AND last_update_date = (select max(last_update_date)
                                    FROM  xxpso_ego_bom_assembly_stg stg2
                                    WHERE stg2.assembly_item_id = stg1.assembly_item_id
                          AND stg2.organization_id = stg1.organization_id
                          AND stg2.structure_name = NVL(stg1.structure_name,'Primary')
                          AND stg2.status_code = gv_processed_flag
                          AND stg2.published_by_etl = 'N'                          
                       );
        

                
                Log_Msg('Bom record found: '||l_rec_bom_assembly_rec.record_id);
                lv_bom_record_found := 'Y';
                
                EXCEPTION
                WHEN OTHERS THEN
                lv_bom_record_found := 'N';
                END;
                
                IF lv_bom_record_found = 'Y' THEN
                    lv_originating_system := l_rec_bom_assembly_rec.assembly_source_system;
                    lv_originating_system_ref := l_rec_bom_assembly_rec.assembly_src_item_number;
                    lv_originating_system_date := l_rec_bom_assembly_rec.last_update_date;
                    IF l_rec_bom_assembly_rec.bom_transaction_type = 'CREATE' THEN
                    	lv_transaction_type       := 'C';
                    ELSIF l_rec_bom_assembly_rec.bom_transaction_type = 'UPDATE' THEN
                    	lv_transaction_type       := 'U';
                    END IF;
                    --lv_transaction_type       := l_rec_bom_assembly_rec.bom_transaction_type;
                    Log_Msg('Record Id of matched BOM Item:'||l_rec_bom_assembly_rec.record_id); 
                ELSE
                    lv_originating_system := 'MDM';
                    lv_originating_system_date := SYSDATE;
                    lv_originating_system_ref  := NULL;--Changes for v1.14
                    lv_transaction_type	       := NULL;--Changes for v1.14                    
                END IF;
                Log_Msg('Bom record details: lv_bom_record_found:'||lv_bom_record_found||
                    ' lv_originating_system:'||lv_originating_system||' lv_originating_system_ref:'||lv_originating_system_ref);
                
                lv_file_line :=   
            -- Header specific parameters
                     replace(lv_originating_system       ,',','~,')   
            ||','||  replace( lv_originating_system_ref  ,',','~,') 
            ||','||  replace( to_char(lv_originating_system_date,'DD-MM-YYYY HH24:MI:SS')  ,',','~,')
            ||','||  replace( c.bill_sequence_id  ,',','~,')        
            ||','||  replace( lv_transaction_type ,',','~,')        
            ||','||  replace( gv_item_entity  ,',','~,')        
            -- Entity specific parameters
            ||','||     replace( c.assembly_item_id ,',','~,')            
            ||','||  replace( c.bom_segment1  ,',','~,')                  
            ||','||  replace( c.structure_name  ,',','~,')                
            ||','||  replace( c.structure_type  ,',','~,')                  
            ||','||  replace( c.organization_code  ,',','~,')            
            ||','||  replace( c.organization_name ,',','~,')             
            ||','||  replace( c.assembly_type  ,',','~,')                
            ||','||  replace( c.component_item_id ,',','~,')           
            ||','||  replace( c.comp_segment1  ,',','~,')                 
            ||','||  replace( c.component_quantity ,',','~,')             
            ||','||  replace( c.operation_seq_num ,',','~,')              
            ||','||  replace( c.item_num   ,',','~,')                     
            ||','||  replace( c.planning_factor  ,',','~,')               
            ||','||  replace(  to_char(c.effectivity_date , 'DD-MM-YYYY HH24:MI:SS') ,',','~,')                
            ||','||  replace(  to_char(c.disable_date   , 'DD-MM-YYYY HH24:MI:SS') ,',','~,');
                   
            UTL_FILE.put_line(l_file,lv_file_line,TRUE );--Changes for v1.8
            l_suc_count := l_suc_count + 1;
            
        --After getting the last updated record update     published_by_etl flag to Y for all earlier and exising records.           
        UPDATE xxpso_ego_bom_assembly_stg
           SET published_by_etl = 'Y'
         WHERE assembly_item_id = c.assembly_item_id
           AND organization_id = c.organization_id
                   AND structure_name = NVL(c.structure_name,'Primary')
                   AND status_code = gv_processed_flag
                   AND published_by_etl = 'N';            
        EXCEPTION
            WHEN OTHERS
            THEN
            Log_Msg('Unable to write to file for Item BOM for Assembly Item Id:'||c.assembly_item_id
            ||' Structure Type:'||c.structure_type
            ||' Component Id:'||c.component_item_id
            ||'-Error Message:' || SQLERRM);
            END;                
        END LOOP;
        
        IF l_rec_count = l_suc_count
        THEN
            Log_Msg('Item BOM File generated successfully');
            
            -- Printing the end of file line  
            UTL_FILE.put_line(l_file, 'Total No of Records - ' ||','|| l_suc_count );
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Success');
        ELSE    
            Log_Msg('Error while generating Item BOM File');
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Failed');
        END IF; 
        
        UTL_FILE.fclose (l_file);
 
        Log_Msg('BOM File generated successfully');
        Log_Msg(' ');
        Log_Msg('End of generate_bom_file Procedure');
        Log_Msg(gcn_print_line); 
        
        -- Writing to Output File 
        Out_Msg(gcn_print_line); 
        Out_Msg('No of records pulled from database for Item BOM           : '|| l_rec_count);
        Out_Msg('No of records inserted into Item BOM CSV File             : '|| l_suc_count);
        Out_Msg(gcn_print_line); 
        Out_Msg(' ');          
        
    EXCEPTION
       WHEN OTHERS
       THEN
          Log_Msg('Error in generate_bom_file procedure -' || SQLERRM);
    
    END generate_bom_file;  
    
/* ********************************************************
   * Procedure: generate_lov_valueset_file
   *
   * Synopsis: This procedure generates file that has valueset names and values.
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
   * Akshay Nayak        1.0                                            06-SEP-2016     
   ************************************************************************************* */       
    PROCEDURE generate_lov_valueset_file 
    IS
        l_file                  UTL_FILE.file_type;
        l_file_name             VARCHAR2(100) := 'XXPSO_ITEM_VALUESET_VALUES';
        l_rec_count             NUMBER              := 0;
        l_suc_count             NUMBER              := 0;           
    
    CURSOR cur_independent_value
    IS
          SELECT ffvs.flex_value_set_name flex_value_set_name,
             ffvs.description description,
             ffvs.flex_value_set_id value_set_id,
             ffv.flex_value value_set_value,
             ffv.flex_value_id value_set_value_id,
             ffvtl.flex_value_meaning value_set_meaning,
             ffvtl.description value_set_description,
             fu1.user_name created_by,
             ffv.creation_date creation_date,
             fu2.user_name last_updated_by,
             ffv.last_update_date last_update_date
        FROM apps.fnd_flex_value_sets ffvs,
             apps.fnd_flex_values ffv,
             apps.fnd_flex_values_tl ffvtl,
             apps.fnd_user fu1,
             apps.fnd_user fu2
       WHERE ffvs.flex_value_set_id IN
                 (SELECT DISTINCT value_set_id
                    FROM apps.ego_attrs_v attr
                   WHERE attr.value_set_id IS NOT NULL
                     AND attr.attr_group_name LIKE 'PSO%'
                     AND attr.enabled_flag = 'Y'
                     AND attr.validation_code_vs = 'I')-- I stands for Independent Value Set
         AND ffvs.flex_value_set_id = ffv.flex_value_set_id
         AND ffv.enabled_flag = 'Y'
         AND ffvtl.flex_value_id(+) = ffv.flex_value_id
         AND ffv.created_by = fu1.user_id
         AND ffv.last_updated_by = fu2.user_id
    ORDER BY ffvs.flex_value_set_id;
        
    CURSOR cur_table_value
    IS
    SELECT ffvs.flex_value_set_name    flex_value_set_name 
          ,ffvs.description         description
          ,ffvs.flex_value_set_id    value_set_id
          ,ffvt.application_table_name    application_table_name
          ,ffvt.additional_where_clause    additional_where_clause
      FROM apps.fnd_flex_value_sets ffvs
             ,apps.fnd_flex_validation_tables ffvt         
      WHERE ffvs.flex_value_set_id IN (SELECT DISTINCT value_set_id
                         FROM apps.ego_attrs_v attr
                        WHERE attr.value_set_id IS NOT NULL
                    AND attr.attr_group_name like 'PSO%' 
                    AND attr.enabled_flag = 'Y'
                    AND attr.validation_code_vs = 'F' -- I stands for Independent Value Set
                       )
        AND ffvs.flex_value_set_id = ffvt.flex_value_set_id
        ORDER BY ffvs.flex_value_set_id; 
        
        TYPE lc_dynamic_lookup         IS REF CURSOR;
        lc_dynamic_lookup_cur         lc_dynamic_lookup;
        lv_query_string            VARCHAR2(4000);    
        lv_lookup_where_clause        VARCHAR2(1000);
    
    lv_lookup_code            fnd_lookup_values.lookup_code%TYPE;
    lv_lookup_meaning        fnd_lookup_values.meaning%TYPE;
    lv_lookup_description        fnd_lookup_values.description%TYPE;
    lv_created_by            fnd_user.user_name%TYPE;
    lv_last_updated_by        fnd_user.user_name%TYPE;
    ld_creation_date        fnd_lookup_values.creation_date%TYPE;
    ld_last_update_date        fnd_lookup_values.last_update_date%TYPE;
    
    --Changes for v1.11 Begin
     CURSOR cur_rel_independent_value
    IS
          SELECT ffvs.flex_value_set_name flex_value_set_name,
             ffvs.description description,
             ffvs.flex_value_set_id value_set_id,
             ffv.flex_value value_set_value,
             ffv.flex_value_id value_set_value_id,
             ffvtl.flex_value_meaning value_set_meaning,
             ffvtl.description value_set_description,
             fu1.user_name created_by,
             ffv.creation_date creation_date,
             fu2.user_name last_updated_by,
             ffv.last_update_date last_update_date
        FROM apps.fnd_flex_value_sets ffvs,
             apps.fnd_flex_values ffv,
             apps.fnd_flex_values_tl ffvtl,
             apps.fnd_user fu1,
             apps.fnd_user fu2,
             apps.fnd_descriptive_flexs flex,
	     apps.fnd_descriptive_flexs_tl flex_tl,
	     apps.fnd_descr_flex_contexts flex_context,
             apps.fnd_descr_flex_column_usages flex_col_usage
       WHERE ffvs.flex_value_set_id = ffv.flex_value_set_id
         AND ffv.enabled_flag = 'Y'
         AND ffvtl.flex_value_id(+) = ffv.flex_value_id
         AND ffv.created_by = fu1.user_id
         AND ffv.last_updated_by = fu2.user_id
         AND flex.application_id = flex_tl.application_id
	 AND flex.descriptive_flexfield_name = flex_tl.descriptive_flexfield_name
	 AND flex_tl.title = 'Item Relationships'
	 AND flex.descriptive_flexfield_name = flex_context.descriptive_flexfield_name
	 AND flex.application_id = flex_context.application_id
	 AND flex_context.enabled_flag = 'Y'
	 AND flex_context.global_flag = 'Y'
	 AND flex_context.application_id = flex_col_usage.application_id(+)
	 AND flex_context.descriptive_flexfield_name = flex_col_usage.descriptive_flexfield_name(+)
	 AND flex_context.descriptive_flex_context_code = flex_col_usage.descriptive_flex_context_code(+)
	 AND flex_col_usage.enabled_flag(+) = 'Y'
         AND flex_col_usage.flex_value_set_id =  ffvs.flex_value_set_id(+)
    ORDER BY ffvs.flex_value_set_id;
        
    CURSOR cur_rel_table_value
    IS
    SELECT ffvs.flex_value_set_name    flex_value_set_name 
          ,ffvs.description         description
          ,ffvs.flex_value_set_id    value_set_id
          ,ffvt.application_table_name    application_table_name
          ,ffvt.additional_where_clause    additional_where_clause
      FROM apps.fnd_flex_value_sets ffvs
             ,apps.fnd_flex_validation_tables ffvt  
             ,apps.fnd_descriptive_flexs flex
	     ,apps.fnd_descriptive_flexs_tl flex_tl
	     ,apps.fnd_descr_flex_contexts flex_context
             ,apps.fnd_descr_flex_column_usages flex_col_usage
      WHERE flex_col_usage.flex_value_set_id =  ffvs.flex_value_set_id(+)
        AND ffvs.flex_value_set_id = ffvt.flex_value_set_id
        AND flex.application_id = flex_tl.application_id
	AND flex.descriptive_flexfield_name = flex_tl.descriptive_flexfield_name
	AND flex_tl.title = 'Item Relationships'
	AND flex.descriptive_flexfield_name = flex_context.descriptive_flexfield_name
	AND flex.application_id = flex_context.application_id
	AND flex_context.enabled_flag = 'Y'
	AND flex_context.global_flag = 'Y'
	AND flex_context.application_id = flex_col_usage.application_id(+)
	AND flex_context.descriptive_flexfield_name = flex_col_usage.descriptive_flexfield_name(+)
	AND flex_context.descriptive_flex_context_code = flex_col_usage.descriptive_flex_context_code(+)
	AND flex_col_usage.enabled_flag(+) = 'Y'
        ORDER BY ffvs.flex_value_set_id;    
    
    --Changes for v1.11 End
            
    BEGIN
        Log_Msg(gcn_print_line);
        Log_Msg('Start of generate_lov_valueset_file Procedure');
    
        l_file_name := l_file_name||'_'||TO_CHAR(gcn_request_id)||'_'||g_sysdate||'.csv';
        
        Log_Msg('DBA Directory name  - ' || gc_dba_directory_name);
        Log_Msg('File Name  - ' || l_file_name);
        
        --Changes for version 1.8 start
        l_file := UTL_FILE.fopen (gc_dba_directory_name, l_file_name, 'w',32767);
    --Changes for version 1.8 end
        
        UTL_FILE.put_line(  l_file,
                           'ValueSetName'                          
                    ||','||    'LOVCode'                         
                    ||','||    'LOVValue'                        
                    ||','||    'CreatedBy'                   
                    ||','||    'CreationDate'                
                    ||','||    'LastUpdatedBy'                
                    ||','||    'LastUpdateDate'   
                    ,TRUE --Changes for v1.8
                         );
                         
        FOR c IN cur_independent_value
        LOOP
            l_rec_count := l_rec_count + 1;
            
            BEGIN        
            UTL_FILE.put_line(  l_file,
                                  replace(  c.flex_value_set_name ,',','~,')
                        ||','||   replace( c.value_set_value  ,',','~,')              
                        ||','||   replace( c.value_set_description  ,',','~,')             
                        ||','||   replace( c.created_by  ,',','~,')                       
                        ||','||   replace( to_char(c.creation_date  , 'DD-MM-YYYY HH24:MI:SS')  ,',','~,')           
                        ||','||   replace( c.last_updated_by ,',','~,')               
                        ||','||   replace( to_char(c.last_update_date  , 'DD-MM-YYYY HH24:MI:SS') ,',','~,')   
                        ,TRUE --Changes for v1.8
                        );   
                                                          
            l_suc_count := l_suc_count + 1;
        EXCEPTION
            WHEN OTHERS
            THEN
            Log_Msg('Unable to write to file for Item Lookup for Value Set Id:'||c.value_set_id||' and value id:'||c.value_set_value_id
            ||' -Error Message:' || SQLERRM);
            END; 
        END LOOP;
        Log_Msg('After inserting cur_independent_value: l_suc_count:'||l_suc_count||' l_rec_count:'||l_rec_count);
        
        FOR c IN cur_table_value
        LOOP
            --Log_Msg('Fetching dynamic data: application_table_name:'||c.application_table_name||
            --        ' additional_where_clause:'||c.additional_where_clause);
        BEGIN        
        lv_lookup_where_clause := SUBSTR(c.additional_where_clause,INSTR(c.additional_where_clause,'lookup_type',1,1));
        IF INSTR(lv_lookup_where_clause,'and',1,1) <> 0 THEN
        lv_lookup_where_clause := SUBSTR(lv_lookup_where_clause,1,INSTR(lv_lookup_where_clause,'and',1,1)-1);
        END IF;
       -- Log_Msg(lv_lookup_where_clause);
        lv_query_string := 'SELECT flv.lookup_code lookup_code,flv.meaning meaning,flv.description description,'||
                       ' fu1.user_name created_by, fu2.user_name last_updated_by, '||
                       ' flv.creation_date creation_date, flv.last_update_date last_update_date FROM '||c.application_table_name || ' flv ' ||
                       ' ,fnd_user fu1,fnd_user fu2' ||
                       ' WHERE flv.'||lv_lookup_where_clause||' AND flv.enabled_flag = ''Y'' AND SYSDATE BETWEEN '||
                       ' NVL(flv.start_date_active,SYSDATE-1) AND NVL(flv.end_date_active,SYSDATE+1) AND flv.created_by = fu1.user_id '||
                       ' AND flv.last_updated_by = fu2.user_id ';
        
           -- Log_Msg(    lv_query_string );   
             OPEN lc_dynamic_lookup_cur FOR lv_query_string;
             LOOP
             FETCH lc_dynamic_lookup_cur INTO lv_lookup_code,lv_lookup_meaning,lv_lookup_description,lv_created_by,lv_last_updated_by
                    ,ld_creation_date,ld_last_update_date;
             EXIT WHEN lc_dynamic_lookup_cur%NOTFOUND;    
             
                     --     Log_Msg(lv_lookup_code||' :: '||lv_lookup_meaning||' :: '||    lv_lookup_description
                     --     ||' :: '||lv_created_by    ||' :: '||lv_last_updated_by);
            l_rec_count := l_rec_count + 1;

            BEGIN        
            UTL_FILE.put_line(  l_file,
                     replace(   c.flex_value_set_name ,',','~,')
                ||','||  replace(  lv_lookup_code  ,',','~,')    
                ||','||  replace(  lv_lookup_meaning  ,',','~,')    
                ||','||  replace(  lv_created_by  ,',','~,')             
                ||','||  replace(  to_char(ld_creation_date  , 'DD-MM-YYYY HH24:MI:SS') ,',','~,')             
                ||','||  replace(  lv_last_updated_by  ,',','~,')    
                ||','||  replace(  to_char(ld_last_update_date  , 'DD-MM-YYYY HH24:MI:SS')  ,',','~,') 
                ,TRUE --Changes for v1.8
                );   

                l_suc_count := l_suc_count + 1;
            EXCEPTION
            WHEN OTHERS
            THEN
            Log_Msg('Unable to write to file for Item Lookup for Table Dependent Value Set Id:'||c.value_set_id
            ||' -Error Message:' || SQLERRM);
            END;                      
             
             END LOOP;
             CLOSE lc_dynamic_lookup_cur;
        EXCEPTION
            WHEN OTHERS
            THEN
            Log_Msg('Unable to write to file for Item Lookup for Value Set Id:'||c.value_set_id
            ||' -Error Message:' || SQLERRM);
            END; 
        END LOOP;    
         Log_Msg('After inserting cur_table_value: l_suc_count:'||l_suc_count||' l_rec_count:'||l_rec_count);
         
         --Changes for v1.11 Begin
        FOR c IN cur_rel_independent_value
        LOOP
            l_rec_count := l_rec_count + 1;
            
            BEGIN        
            UTL_FILE.put_line(  l_file,
                                  replace(  c.flex_value_set_name ,',','~,')
                        ||','||   replace( c.value_set_value  ,',','~,')              
                        ||','||   replace( c.value_set_description  ,',','~,')             
                        ||','||   replace( c.created_by  ,',','~,')                       
                        ||','||   replace( to_char(c.creation_date  , 'DD-MM-YYYY HH24:MI:SS')  ,',','~,')           
                        ||','||   replace( c.last_updated_by ,',','~,')               
                        ||','||   replace( to_char(c.last_update_date  , 'DD-MM-YYYY HH24:MI:SS') ,',','~,')   
                        ,TRUE --Changes for v1.8
                        );   
                                                          
            l_suc_count := l_suc_count + 1;
        EXCEPTION
            WHEN OTHERS
            THEN
            Log_Msg('Unable to write to file for Item Lookup for Value Set Id:'||c.value_set_id||' and value id:'||c.value_set_value_id
            ||' -Error Message:' || SQLERRM);
            END; 
        END LOOP;
        Log_Msg('After inserting cur_independent_value: l_suc_count:'||l_suc_count||' l_rec_count:'||l_rec_count);
        
        FOR c IN cur_rel_table_value
        LOOP
            --Log_Msg('Fetching dynamic data: application_table_name:'||c.application_table_name||
            --        ' additional_where_clause:'||c.additional_where_clause);
        BEGIN        
        lv_lookup_where_clause := SUBSTR(c.additional_where_clause,INSTR(c.additional_where_clause,'lookup_type',1,1));
        IF INSTR(lv_lookup_where_clause,'and',1,1) <> 0 THEN
        lv_lookup_where_clause := SUBSTR(lv_lookup_where_clause,1,INSTR(lv_lookup_where_clause,'and',1,1)-1);
        END IF;
       -- Log_Msg(lv_lookup_where_clause);
        lv_query_string := 'SELECT flv.lookup_code lookup_code,flv.meaning meaning,flv.description description,'||
                       ' fu1.user_name created_by, fu2.user_name last_updated_by, '||
                       ' flv.creation_date creation_date, flv.last_update_date last_update_date FROM '||c.application_table_name || ' flv ' ||
                       ' ,fnd_user fu1,fnd_user fu2' ||
                       ' WHERE flv.'||lv_lookup_where_clause||' AND flv.enabled_flag = ''Y'' AND SYSDATE BETWEEN '||
                       ' NVL(flv.start_date_active,SYSDATE-1) AND NVL(flv.end_date_active,SYSDATE+1) AND flv.created_by = fu1.user_id '||
                       ' AND flv.last_updated_by = fu2.user_id ';
        
           -- Log_Msg(    lv_query_string );   
             OPEN lc_dynamic_lookup_cur FOR lv_query_string;
             LOOP
             FETCH lc_dynamic_lookup_cur INTO lv_lookup_code,lv_lookup_meaning,lv_lookup_description,lv_created_by,lv_last_updated_by
                    ,ld_creation_date,ld_last_update_date;
             EXIT WHEN lc_dynamic_lookup_cur%NOTFOUND;    
             
                     --     Log_Msg(lv_lookup_code||' :: '||lv_lookup_meaning||' :: '||    lv_lookup_description
                     --     ||' :: '||lv_created_by    ||' :: '||lv_last_updated_by);
            l_rec_count := l_rec_count + 1;

            BEGIN        
            UTL_FILE.put_line(  l_file,
                     replace(   c.flex_value_set_name ,',','~,')
                ||','||  replace(  lv_lookup_code  ,',','~,')    
                ||','||  replace(  lv_lookup_meaning  ,',','~,')    
                ||','||  replace(  lv_created_by  ,',','~,')             
                ||','||  replace(  to_char(ld_creation_date  , 'DD-MM-YYYY HH24:MI:SS') ,',','~,')             
                ||','||  replace(  lv_last_updated_by  ,',','~,')    
                ||','||  replace(  to_char(ld_last_update_date  , 'DD-MM-YYYY HH24:MI:SS')  ,',','~,') 
                ,TRUE --Changes for v1.8
                );   

                l_suc_count := l_suc_count + 1;
            EXCEPTION
            WHEN OTHERS
            THEN
            Log_Msg('Unable to write to file for Item Lookup for Table Dependent Value Set Id:'||c.value_set_id
            ||' -Error Message:' || SQLERRM);
            END;                      
             
             END LOOP;
             CLOSE lc_dynamic_lookup_cur;
        EXCEPTION
            WHEN OTHERS
            THEN
            Log_Msg('Unable to write to file for Item Lookup for Value Set Id:'||c.value_set_id
            ||' -Error Message:' || SQLERRM);
            END; 
        END LOOP;    
         Log_Msg('After inserting cur_table_value: l_suc_count:'||l_suc_count||' l_rec_count:'||l_rec_count);         
         --Changes for v1.11 End
         
        IF l_rec_count = l_suc_count
        THEN
            Log_Msg('LOV Value Reference File generated successfully');
            
            -- Printing the end of file line 
            UTL_FILE.put_line(l_file, 'Total No of Records - ' ||','|| l_suc_count );
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Success');
        ELSE    
            Log_Msg('Error while generating LOV Value Reference File File');
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Failed');
        END IF; 
        
        UTL_FILE.fclose (l_file);
 
        Log_Msg('LOV Value Reference File generated successfully');
        Log_Msg(' ');
        Log_Msg('End of generate_lov_valueset_file Procedure');
        Log_Msg(gcn_print_line); 
        
        -- Writing to Output File 
        Out_Msg(gcn_print_line); 
        Out_Msg('No of records pulled from database for LOV Value           : '|| l_rec_count);
        Out_Msg('No of records inserted into LOV Value Reference File             : '|| l_suc_count);
        Out_Msg(gcn_print_line); 
        Out_Msg(' ');             
        
        
    EXCEPTION
       WHEN OTHERS
       THEN
          Log_Msg('Error in generate_lov_valueset_file procedure -' || SQLERRM);
    
    END generate_lov_valueset_file;      

    --Changes for v1.6 Begin
/* ********************************************************
   * Procedure: generate_item_attach_doc_file
   *
   * Synopsis: This procedure is to generate item attached document files
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
   * Akshay Nayak    1.0                                            23-Sep-2016
   ************************************************************************************* */   
    PROCEDURE generate_item_attach_doc_file
    IS
        l_file               UTL_FILE.file_type;
        l_file_name     VARCHAR2(100)       := 'XXPSO_ITEM_ATTACHED_DOC'; 
        l_rec_count     NUMBER              := 0;
        l_suc_count     NUMBER              := 0;
        
        CURSOR c_item_attached_doc
        IS
     select msib.inventory_item_id inventory_item_id
        ,fd.document_id document_id
        ,fdc.user_name  user_name
        ,fdtl.title title
        ,fdtl.description description
        ,fd.media_id    media_id
        ,fd.datatype_id    datatype_id
        ,fdlong.long_text long_text
        ,fu1.user_name                      created_by_name
        ,fad.creation_date                 creation_date        
        ,fu2.user_name                      last_updated_by_name
        ,fad.last_update_date              last_update_date    
        from apps.mtl_system_items_b msib
        ,apps.fnd_attached_documents fad
        ,apps.fnd_documents fd
        ,apps.fnd_documents_tl fdtl
        ,apps.fnd_document_categories_tl fdc
        ,apps.fnd_documents_long_text fdlong
        ,apps.fnd_user fu1
        ,apps.fnd_user fu2
        where msib.inventory_item_id = fad.pk2_value
          and msib.organization_id = fad.pk1_value
          and fad.entity_name = 'MTL_SYSTEM_ITEMS'
          and fad.document_id = fd.document_id
          and fd.category_id = fdc.category_id
          and fd.document_id = fdtl.document_id
          and fd.datatype_id = 2 -- Datatype value for the long attachements.
          and fd.media_id = fdlong.media_id
          and fad.created_by = fu1.user_id
          and fad.last_updated_by = fu2.user_id
          and fad.last_update_date BETWEEN g_start_date AND g_end_date
           order by fd.document_id;
           
    BEGIN
    
        Log_Msg(gcn_print_line);
        Log_Msg('Start of generate_item_attach_doc_file');
    
        l_file_name := l_file_name ||'_'|| gcn_request_id ||'_'|| g_sysdate ||'.csv';

        
        Log_Msg('DBA Directory Name - ' || gc_dba_directory_name);
        Log_Msg('File Name  - ' || l_file_name);
        
        --Changes for version 1.8 start
        l_file := UTL_FILE.fopen (gc_dba_directory_name, l_file_name, 'w',32767);
    --Changes for version 1.8 end

        
        UTL_FILE.put_line(  l_file,
                       'ItemId'                               
                ||','||    'DocumentID'                       
                ||','||    'DocumentCategory'        
                ||','||    'DocumentTitle'           
                ||','||    'DocumentDescription'     
                ||','||    'DocumentLongText'      
                ||','||    'CreationDate'            
                ||','||    'CreatedBy'               
                ||','||    'LastUpdateDate'          
                ||','||    'LastUpdatedBy'  
                ,TRUE --Changes for v1.8
                             );                         
        
        FOR c IN c_item_attached_doc
        LOOP
            l_rec_count := l_rec_count + 1;
            
            BEGIN
                UTL_FILE.PUT_LINE(  l_file,
                                  replace( c.inventory_item_id ,',','~,')                                             
                        ||','||   replace( c.document_id ,',','~,')                         
                        ||','||   replace( c.user_name ,',','~,')                  
                        ||','||   replace( c.title ,',','~,')                   
                        ||','||   replace( c.description ,',','~,')                
                        ||','||   replace( TRIM(c.long_text) ,',','~,')                  
                        ||','||   replace( to_char(c.creation_date , 'DD-MM-YYYY HH24:MI:SS')   ,',','~,')           
                        ||','||   replace( c.created_by_name  ,',','~,')           
                        ||','||   replace( to_char(c.last_update_date , 'DD-MM-YYYY HH24:MI:SS')  ,',','~,')        
                        ||','||   replace( c.last_updated_by_name ,',','~,')     
                        , TRUE --Changes for v1.8                          
                            );
                                 
                l_suc_count := l_suc_count + 1;
                
            EXCEPTION
                WHEN OTHERS
                THEN
                    Log_Msg('Unable to write to file for Items with Inventory Item Id:'||c.inventory_item_id||
                    '- ' || SQLERRM);
            END;
        END LOOP;

  
        
        
        IF l_rec_count = l_suc_count
        THEN
            Log_Msg('Item Attachment document file generated successfully');
            
            -- Printing the end of file line  
            UTL_FILE.put_line(l_file, 'Total No of Records - ' ||','|| l_suc_count );
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Success');
        ELSE    
            Log_Msg('Error while generating Item Attachment document File');
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Failed');
        END IF; 
                
        
        UTL_FILE.fclose (l_file);
        
        Log_Msg(' ');
        Log_Msg('End of generate_item_attach_doc_file Procedure');
        Log_Msg(gcn_print_line); 
                
        -- Writing to Output File 
        Out_Msg(gcn_print_line); 
        Out_Msg('No of records pulled from database for Item Attachment document file          : '|| l_rec_count);
        Out_Msg('No of records inserted into Item Attachment document CSV File              : '|| l_suc_count);
        Out_Msg(gcn_print_line); 
        Out_Msg(' ');
        
        
    EXCEPTION
       WHEN OTHERS
       THEN
          Log_Msg('Error in generate_item_attach_doc_file procedure - ' || SQLERRM);
    
    END generate_item_attach_doc_file; 
    
/* ********************************************************
   * Procedure: generate_item_category_file
   *
   * Synopsis: This procedure is to generate item categories related file
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
   * Akshay Nayak    1.0                                            17-Sep-2016
   ************************************************************************************* */   
    PROCEDURE generate_item_category_file
    IS
        l_file               UTL_FILE.file_type;
        l_file_name     VARCHAR2(100)       := 'XXPSO_ITEM_ALTERNATE_CATALOG'; 
        l_rec_count     NUMBER              := 0;
        l_suc_count     NUMBER              := 0;
        
        CURSOR c_item_catalog
        IS
            SELECT msib.inventory_item_id inventory_item_id
              ,msib.segment1 item_number 
              ,mp.organization_code organization_code 
              ,hou.name organization_name 
              ,mcs.category_set_name  category_set_name
              ,mic.category_id  category_id
              ,fu1.user_name                      created_by_name
              ,mic.creation_date                 creation_date        
              ,fu2.user_name                      last_updated_by_name
              ,mic.last_update_date              last_update_date              
            FROM apps.mtl_system_items_b msib 
              ,apps.mtl_system_items_tl mstl 
              ,apps.mtl_parameters mp 
              ,apps.hr_organization_units hou 
              --,apps.xxpso_ego_item_change_gtt item_gtt   ---Since this is an individual entity this would not be part of Item changes.
              ,apps.mtl_item_categories mic 
              ,apps.mtl_category_sets mcs
              ,apps.fnd_user                      fu1
              ,apps.fnd_user                      fu2              
            WHERE mstl.inventory_item_id = msib.inventory_item_id
            AND mstl.organization_id     = msib.organization_id
            AND msib.organization_id     = mp.organization_id
            AND mp.organization_id       = hou.organization_id
            --AND msib.inventory_item_id   = item_gtt.inventory_item_id
            AND mic.last_update_date BETWEEN g_start_date AND g_end_date
            AND mic.inventory_item_id    = msib.inventory_item_id
            AND mic.organization_id      = msib.organization_id
            AND mic.category_set_id      = mcs.category_set_id
            AND mic.created_by                 = fu1.user_id 
            AND mic.last_updated_by            = fu2.user_id 
            ORDER BY msib.inventory_item_id;                
           
    BEGIN
    
        Log_Msg(gcn_print_line);
        Log_Msg('Start of generate_item_category_file');
    
        l_file_name := l_file_name ||'_'|| gcn_request_id ||'_'|| g_sysdate ||'.csv';

        
        Log_Msg('DBA Directory Name - ' || gc_dba_directory_name);
        Log_Msg('File Name  - ' || l_file_name);
        
        --Changes for version 1.8 start
        l_file := UTL_FILE.fopen (gc_dba_directory_name, l_file_name, 'w',32767);
    --Changes for version 1.8 end

        
        UTL_FILE.put_line(  l_file,
                       'ItemId'                               
                ||','||    'ItemNumber'                       
                ||','||    'OrganizationCode'        
                ||','||    'OrganizationName'        
                ||','||    'CategorySetName'         
                ||','||    'CategoryId'             
                ||','||    'CreationDate'            
                ||','||    'CreatedBy'               
                ||','||    'LastUpdateDate'          
                ||','||    'LastUpdatedBy' 
                ,TRUE --Changes for v1.8
                             );                         
        
        FOR c IN c_item_catalog
        LOOP
            l_rec_count := l_rec_count + 1;
            
            BEGIN
                UTL_FILE.put_line(  l_file,
                                  replace( c.inventory_item_id ,',','~,')                                        
                        ||','||   replace( c.item_number ,',','~,')                         
                        ||','||   replace( c.organization_code  ,',','~,')         
                        ||','||   replace( c.organization_name ,',','~,')          
                        ||','||   replace( c.category_set_name ,',','~,')         
                        ||','||   replace( c.category_id  ,',','~,')          
                        ||','||   replace( to_char(c.creation_date , 'DD-MM-YYYY HH24:MI:SS') ,',','~,')             
                        ||','||   replace( c.created_by_name ,',','~,')            
                        ||','||   replace( to_char(c.last_update_date , 'DD-MM-YYYY HH24:MI:SS'),',','~,')          
                        ||','||   replace( c.last_updated_by_name ,',','~,')     
                        ,TRUE --Changes for v1.8
                            );
                                 
                l_suc_count := l_suc_count + 1;
                
            EXCEPTION
                WHEN OTHERS
                THEN
                    Log_Msg('Unable to write to file for Items with Inventory Item Id:'||c.inventory_item_id||
                    '- ' || SQLERRM);
            END;
        END LOOP;

  
        
        
        IF l_rec_count = l_suc_count
        THEN
            Log_Msg('Item Alternate Catalog generated successfully');
            
            -- Printing the end of file line  
            UTL_FILE.put_line(l_file, 'Total No of Records - ' ||','|| l_suc_count );
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Success');
        ELSE    
            Log_Msg('Error while generating Item Alternat Catalog File');
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Failed');
        END IF; 
                
        
        UTL_FILE.fclose (l_file);
        
        Log_Msg(' ');
        Log_Msg('End of generate_item_category_file Procedure');
        Log_Msg(gcn_print_line); 
                
        -- Writing to Output File 
        Out_Msg(gcn_print_line); 
        Out_Msg('No of records pulled from database for Item Alternate Catalog          : '|| l_rec_count);
        Out_Msg('No of records inserted into Item Alternate Catalog CSV File              : '|| l_suc_count);
        Out_Msg(gcn_print_line); 
        Out_Msg(' ');
        
        
    EXCEPTION
       WHEN OTHERS
       THEN
          Log_Msg('Error in generate_item_category_file procedure - ' || SQLERRM);
    
    END generate_item_category_file; 
  
/* ********************************************************
   * Procedure: generate_master_category_file
   *
   * Synopsis: This procedure is to generate master category file
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
   * Akshay Nayak    1.0                                            17-Sep-2016
   ************************************************************************************* */   
    PROCEDURE generate_master_category_file
    IS
        l_file               UTL_FILE.file_type;
        l_file_name     VARCHAR2(100)       := 'XXPSO_CATEGORIES'; 
        l_rec_count     NUMBER              := 0;
        l_suc_count     NUMBER              := 0;
        
        CURSOR c_master_categories
        IS
           SELECT FIF.APPLICATION_ID APPLICATION_ID,
              FIF.ID_FLEX_CODE ID_FLEX_CODE ,
              FIF.ID_FLEX_NAME ID_FLEX_NAME,
              FIF.APPLICATION_TABLE_NAME APPLICATION_TABLE_NAME,
              FIF.DESCRIPTION DESCRIPTION,
              FIFS.ID_FLEX_NUM ID_FLEX_NUM,
              FIFS.ID_FLEX_STRUCTURE_CODE ID_FLEX_STRUCTURE_CODE,
              FIFSTL.ID_FLEX_STRUCTURE_NAME structure_internal_name ,
              FIFSTL.DESCRIPTION structure_name ,
              xc.SEGMENT1 SEGMENT1 ,
              xc.SEGMENT1_DESC SEGMENT1_DESC ,
              xc.SEGMENT2 SEGMENT2 ,
              xc.SEGMENT2_DESC SEGMENT2_DESC ,
              xc.SEGMENT3 SEGMENT3 ,
              xc.SEGMENT3_DESC SEGMENT3_DESC ,
              xc.SEGMENT4 SEGMENT4 ,
              xc.SEGMENT4_DESC SEGMENT4_DESC ,
              xc.SEGMENT5 SEGMENT5 ,
              xc.SEGMENT5_DESC SEGMENT5_DESC ,
              xc.SEGMENT6 SEGMENT6 ,
              xc.SEGMENT6_DESC SEGMENT6_DESC ,
              xc.SEGMENT7 SEGMENT7 ,
              xc.SEGMENT7_DESC SEGMENT7_DESC ,
              xc.SEGMENT8 SEGMENT8 ,
              xc.SEGMENT8_DESC SEGMENT8_DESC ,
              xc.SEGMENT9 SEGMENT9 ,
              xc.SEGMENT9_DESC SEGMENT9_DESC ,
              xc.SEGMENT10 SEGMENT10,
              xc.SEGMENT10_DESC SEGMENT10_DESC ,
              xc.SEGMENT11 SEGMENT11,
              xc.SEGMENT11_DESC SEGMENT11_DESC ,
              xc.SEGMENT12 SEGMENT12,
              xc.SEGMENT12_DESC SEGMENT12_DESC ,
              xc.SEGMENT13 SEGMENT13,
              xc.SEGMENT13_DESC SEGMENT13_DESC ,
              xc.SEGMENT14 SEGMENT14,
              xc.SEGMENT14_DESC SEGMENT14_DESC ,
              xc.SEGMENT15 SEGMENT15,
              xc.SEGMENT15_DESC SEGMENT15_DESC ,
              xc.SEGMENT16 SEGMENT16,
              xc.SEGMENT16_DESC SEGMENT16_DESC ,
              xc.SEGMENT17 SEGMENT17,
              xc.SEGMENT17_DESC SEGMENT17_DESC ,
              xc.SEGMENT18 SEGMENT18,
              xc.SEGMENT18_DESC SEGMENT18_DESC ,
              xc.SEGMENT19 SEGMENT19,
              xc.SEGMENT19_DESC SEGMENT19_DESC ,
              xc.SEGMENT20 SEGMENT20,
              xc.SEGMENT20_DESC SEGMENT20_DESC ,
              xc.CATEGORY_ID  CATEGORY_ID ,
              MCS.CATEGORY_SET_NAME CATEGORY_SET_NAME
            FROM apps.FND_ID_FLEXS FIF ,
              apps.FND_ID_FLEX_STRUCTURES FIFS ,
              apps.FND_ID_FLEX_STRUCTURES_TL FIFSTL ,
              xxpso.xxpso_ego_categories_stg XC ,
              apps.mtl_category_sets mcs , 
              apps.mtl_category_set_valid_cats mcsvc              
            WHERE FIF.APPLICATION_ID        = FIFS.APPLICATION_ID
            AND FIF.ID_FLEX_CODE            = FIFS.ID_FLEX_CODE
            AND FIF.ID_FLEX_CODE            = 'MCAT'
            AND FIF.ID_FLEX_NAME            = 'Item Categories'
            AND FIFS.ID_FLEX_STRUCTURE_CODE = XC.STRUCTURE_CODE
            AND FIFSTL.APPLICATION_ID       = FIFS.APPLICATION_ID
            AND FIFSTL.ID_FLEX_CODE         = FIFS.ID_FLEX_CODE
            AND FIFSTL.ID_FLEX_NUM          = FIFS.ID_FLEX_NUM
            AND FIFS.ID_FLEX_NUM            = XC.STRUCTURE_ID
            AND FIFS.ID_FLEX_NUM            = MCS.STRUCTURE_ID
            AND XC.CATEGORY_ID              = MCSVC.CATEGORY_ID
            AND MCS.CATEGORY_SET_ID         = MCSVC.CATEGORY_SET_ID            
            AND FIFS.ENABLED_FLAG           = 'Y'
            AND XC.ENABLED_FLAG             = 'Y'
            AND XC.last_update_date BETWEEN g_start_date AND g_end_date
            ORDER BY XC.CATEGORY_ID;         
           
    BEGIN
    
        Log_Msg(gcn_print_line);
        Log_Msg('Start of generate_master_category_file');
    
        l_file_name := l_file_name ||'_'|| gcn_request_id ||'_'|| g_sysdate ||'.csv';

        
        Log_Msg('DBA Directory Name - ' || gc_dba_directory_name);
        Log_Msg('File Name  - ' || l_file_name);
        
        --Changes for version 1.8 start
        l_file := UTL_FILE.fopen (gc_dba_directory_name, l_file_name, 'w',32767);
    --Changes for version 1.8 end

        
        UTL_FILE.put_line(  l_file,
                       'StructureName'                                       
                ||','||    'CategorySetName'                       
                ||','||    'CategoryId'                            
                ||','||    'Segment1'  
                ||','||    'Segment1Desc'                
                ||','||    'Segment2'                   
                ||','||    'Segment2Desc'                                
                ||','||    'Segment3'                   
                ||','||    'Segment3Desc'                                
                ||','||    'Segment4'                   
                ||','||    'Segment4Desc'                                
                ||','||    'Segment5'                   
                ||','||    'Segment5Desc'                                
                ||','||    'Segment6'                   
                ||','||    'Segment6Desc'                                
                ||','||    'Segment7'                   
                ||','||    'Segment7Desc'                                
                ||','||    'Segment8'                   
                ||','||    'Segment8Desc'                                
                ||','||    'Segment9'                   
                ||','||    'Segment9Desc'                                
                ||','||    'Segment10'    
                ||','||    'Segment10Desc'    
                ||','||    'Segment11'    
                ||','||    'Segment11Desc'    
                ||','||    'Segment12'    
                ||','||    'Segment12Desc'    
                ||','||    'Segment13'    
                ||','||    'Segment13Desc'    
                ||','||    'Segment14'    
                ||','||    'Segment14Desc'    
                ||','||    'Segment15'    
                ||','||    'Segment15Desc'
                ||','||    'Segment16'    
                ||','||    'Segment16Desc'    
                ||','||    'Segment17'    
                ||','||    'Segment17Desc'    
                ||','||    'Segment18'    
                ||','||    'Segment18Desc'        
                ||','||    'Segment19'    
                ||','||    'Segment19Desc'    
                ||','||    'Segment20'    
                ||','||    'Segment20Desc'                        
                ,TRUE --Changes for v1.8
              
                             );                         
        
        FOR c IN c_master_categories
        LOOP
            l_rec_count := l_rec_count + 1;
            
            BEGIN
                UTL_FILE.put_line(  l_file,
                                     replace( c.structure_name ,',','~,')                       
                        ||','||   replace( c.category_set_name ,',','~,')              
                        ||','||   replace( c.category_id  ,',','~,')                 
                        ||','||   replace( c.segment1 ,',','~,')  
                        ||','||   replace( c.segment1_desc ,',','~,')                         
                        ||','||   replace( c.segment2,',','~,') 
                        ||','||   replace( c.segment2_desc ,',','~,')                             
                        ||','||   replace( c.segment3 ,',','~,') 
                        ||','||   replace( c.segment3_desc ,',','~,')                             
                        ||','||   replace( c.segment4 ,',','~,')       
                        ||','||   replace( c.segment4_desc ,',','~,')                             
                        ||','||   replace( c.segment5 ,',','~,')       
                        ||','||   replace( c.segment5_desc ,',','~,')                             
                        ||','||   replace( c.segment6 ,',','~,')       
                        ||','||   replace( c.segment6_desc ,',','~,')                             
                        ||','||   replace( c.segment7 ,',','~,')       
                        ||','||   replace( c.segment7_desc ,',','~,')                             
                        ||','||   replace( c.segment8  ,',','~,')      
                        ||','||   replace( c.segment8_desc ,',','~,')                             
                        ||','||   replace( c.segment9 ,',','~,')       
                        ||','||   replace( c.segment9_desc ,',','~,')                             
                        ||','||   replace( c.segment10  ,',','~,') 
                        ||','||   replace( c.segment10_desc ,',','~,')                             
                        ||','||   replace( c.segment11 ,',','~,')      
                        ||','||   replace( c.segment11_desc ,',','~,')                             
                        ||','||   replace( c.segment12,',','~,')       
                        ||','||   replace( c.segment12_desc ,',','~,')                             
                        ||','||   replace( c.segment13 ,',','~,')      
                        ||','||   replace( c.segment13_desc ,',','~,')                             
                        ||','||   replace( c.segment14 ,',','~,')      
                        ||','||   replace( c.segment14_desc ,',','~,')                             
                        ||','||   replace( c.segment15 ,',','~,')      
                        ||','||   replace( c.segment15_desc ,',','~,')                             
                        ||','||   replace( c.segment16 ,',','~,')      
                        ||','||   replace( c.segment16_desc ,',','~,')                             
                        ||','||   replace( c.segment17 ,',','~,')      
                        ||','||   replace( c.segment17_desc ,',','~,')                             
                        ||','||   replace( c.segment18  ,',','~,')     
                        ||','||   replace( c.segment18_desc ,',','~,')                             
                        ||','||   replace( c.segment19 ,',','~,')      
                        ||','||   replace( c.segment19_desc ,',','~,')                             
                        ||','||   replace( c.segment20  ,',','~,')       
                        ||','||   replace( c.segment20_desc ,',','~,')                             
                         ,TRUE --Changes for v1.8
                            );
                                 
                l_suc_count := l_suc_count + 1;
                
            EXCEPTION
                WHEN OTHERS
                THEN
                    Log_Msg('Unable to write to file for Master Category for category Id:'||c.category_id||
                    '- ' || SQLERRM);
            END;
        END LOOP;

  
        
        
        IF l_rec_count = l_suc_count
        THEN
            Log_Msg('Master Categories generated successfully');
            
            -- Printing the end of file line  
            UTL_FILE.put_line(l_file, 'Total No of Records - ' ||','|| l_suc_count );
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Success');
        ELSE    
            Log_Msg('Error while generating Master Categories File');
            UTL_FILE.put_line(  g_trigger_file,l_file_name||',Failed');
        END IF; 
                
        
        UTL_FILE.fclose (l_file);
        
        Log_Msg(' ');
        Log_Msg('End of generate_master_category_file Procedure');
        Log_Msg(gcn_print_line); 
                
        -- Writing to Output File 
        Out_Msg(gcn_print_line); 
        Out_Msg('No of records pulled from database for Master Categories          : '|| l_rec_count);
        Out_Msg('No of records inserted into Master Categories CSV File              : '|| l_suc_count);
        Out_Msg(gcn_print_line); 
        Out_Msg(' ');
        
        
    EXCEPTION
       WHEN OTHERS
       THEN
          Log_Msg('Error in generate_master_category_file procedure - ' || SQLERRM);
    
    END generate_master_category_file;     
    --Changes for v1.6 End
    
    
   /* ************************************************************************************
   * Procedure: main_p
   *
   * Synopsis: This procedure will be called from conc prog - XXPSO PDH Publish Batch Outbound Data
   *
   * PARAMETERS: 
   *   OUT: 
   *        p_errbuf                VARCHAR2        -- Buffer variable for error message
   *        p_retcode               NUMBER          -- Return code variable to indicate program completion status 
   *   IN:
   *        p_debug_flag            VARCHAR2        -- Its value may be 'Y' or 'N'
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            23-Jun-2016
   ************************************************************************************* */        
    PROCEDURE main_p(p_errbuf       OUT   NOCOPY   VARCHAR2,
                     p_retcode      OUT   NOCOPY   VARCHAR2,
                     p_start_date   IN             VARCHAR2,
                     p_end_date     IN             VARCHAR2,
                     p_debug_flag   IN             VARCHAR2
                     )
    IS
    
        lv_prog_name        VARCHAR2 (200)      := NULL;
        lv_str              VARCHAR2 (1000)     := '************************';
        lv_str2             VARCHAR2 (1000)     := '---------';
    
    BEGIN
    
        gc_debug_flag       := p_debug_flag;
        g_start_date        := FND_DATE.CANONICAL_TO_DATE(p_start_date);
        g_end_date          := FND_DATE.CANONICAL_TO_DATE(p_end_date);
        g_sysdate        := TO_CHAR(SYSDATE,'DDMMYYYY_HH24MISS');
    
        BEGIN
            SELECT user_concurrent_program_name
              INTO lv_prog_name
              FROM fnd_concurrent_programs_tl
             WHERE concurrent_program_id = gcn_program_id
               AND language = 'US';
        EXCEPTION
            WHEN OTHERS THEN
                lv_prog_name := NULL;
        END;
        
        -- Printing log  
        Log_Msg('Start of Main Procedure');
        Log_Msg('p_start_date   -' || p_start_date);
        Log_Msg('g_start_date   -' || to_char(g_start_date,'DD-MM-YYYY HH12:MI:SS'));
        Log_Msg('p_end_date     -' || p_end_date);
        Log_Msg('g_end_date     -' || to_char(g_end_date,'DD-MM-YYYY HH12:MI:SS'));
        Log_Msg('p_debug_flag   -' || p_debug_flag);
        Log_Msg('g_sysdate      -' || g_sysdate);
        Log_Msg(' ');
        
        -- Generating a report in Output File
        Out_Msg( lv_str || ' ' || lv_prog_name || ' ' || lv_str );
        Out_Msg( '  ');
        Out_Msg( 'Request ID            : ' || gcn_request_id);
        Out_Msg( 'Program Run Date      : ' || TO_CHAR (SYSDATE, 'MM-DD-RRRR'));
        Out_Msg( 'Parameters -----------------------------------------------------' );
        Out_Msg( 'p_start_date          : ' || p_start_date);
        Out_Msg( 'p_end_date            : ' || p_end_date);
        Out_Msg( 'p_debug_flag          : ' || p_debug_flag);
        Out_Msg( '  ');
    
        --Opening the trigger file

        g_trigger_file_name := g_trigger_file_name || g_sysdate || '.csv';
        --Changes for version 1.8 start
        g_trigger_file := UTL_FILE.fopen (gc_dba_directory_name, g_trigger_file_name, 'w',32767);
        --Changes for version 1.8 end
        UTL_FILE.put_line(  g_trigger_file,'File Name,Status');

        
        --Changes for v1.5
        populate_gtt;
        
        -- Calling procedures to generate data files 
        generate_item_file;  
        generate_uda_file;
        generate_xref_file;
        generate_bom_file;
        generate_relationship_file;
        --Changes for v1.6 Begin
        generate_item_category_file;
        generate_master_category_file;
        --Changes for v1.6 End
        generate_lov_valueset_file;
        generate_item_attach_doc_file;
        
        COMMIT;

        --Closing the trigger file
        UTL_FILE.fclose (g_trigger_file);

        
        Log_Msg(' ');
        Log_Msg('End of Main Procedure');
        Log_Msg(gcn_print_line); 
    
    END main_p;

END XXPSO_PDH_BATCH_OUTBOUND_PKG;



/
SHOW ERROR

EXEC APPS.XXPSO_INSTALL_PK.VERIFY('XXPSO_PDH_BATCH_OUTBOUND_PKG');
EXIT;
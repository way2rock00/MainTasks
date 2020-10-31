WHENEVER SQLERROR EXIT FAILURE

create or replace PACKAGE BODY XXPSO_PRODUCT_UTILITY_PKG
AS
--+============================================================|
--| Module Name: PRODUCT UTILITY PACKAGE
--|
--| File Name: XXPSO_PRODUCT_UTILITY_PKG.sql
--|
--| Description: This package will be used for adding custom 
--| procedures and functions that will be used PIM.
--|
--| Date: 09-Jun-2016
--|
--| Author: Akshay Nayak
--|
--| Usage: PRODUCT UTILITY PACKAGE
--| Copyright: Pearson
--|
--| All rights reserved.
--|
--+============================================================|
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 09-Jun-2016  Akshay Nayak           Initial Creation
--| 29-Aug-2016  Narendra Mishra        Added XML generation common procedures and functions 
--| 22-Sep-2016  Akshay Nayak           GHEPMv2 Changes
--|  Akshay Nayak         | 26-Sep-2016   | 1.2      | Integration related changes 
--|  Prashanthi Bukkittu  | 19-Oct-2016   | 1.3      | Changes to related items xml generation 
--|  Prashanthi Bukkittu  | 20-Oct-2016   | 1.4      | Fixed the issue "CSV was extracting all the records instead of extracting the records in the given date range"
--|  Akshay Nayak         | 20-Oct-2016   | 1.5      | Fix for issue identified by Betsy. Invalid ISBN value.
--|  Akshay Nayak	  | 25-Oct-2016   | 1.6      | Fix for Defect 2492. MDM -> TEP scenario for record updated in TEP
--+============================================================|
    gv_processed_flag    CONSTANT     VARCHAR2(1)    := 'P';
    gv_item_entity         CONSTANT    VARCHAR2(20) := 'ITEM'; 
    gv_bom_entity         CONSTANT    VARCHAR2(20) := 'BOM';
    gv_related_item_entity     CONSTANT    VARCHAR2(20) := 'RELATED_ITEM';
    
    FUNCTION xxpso_get_relation_addnt_attr ( p_in_inventory_item_id         IN     NUMBER
                                            ,p_in_related_item_id           IN     NUMBER
                                            ,p_in_org_id                    IN     NUMBER
                                            ,p_in_relationship_type_id      IN     NUMBER
                                        )
    RETURN VARCHAR2
    IS
        lv_attribute_value          VARCHAR2(240) := NULL;
        lv_column_name              VARCHAR2(20);
        
        BEGIN
           BEGIN
            BEGIN

            SELECT application_column_name
              INTO lv_column_name
            FROM apps.fnd_descr_flex_contexts contexts,
              apps.fnd_descr_flex_column_usages segments
            WHERE contexts.descriptive_flexfield_name  = 'MTL_RELATED_ITEMS'
            AND contexts.enabled_flag                  = 'Y'
            AND segments.application_id                = contexts.application_id
            AND segments.descriptive_flexfield_name    = contexts.descriptive_flexfield_name
            AND segments.descriptive_flex_context_code = contexts.descriptive_flex_context_code
            AND segments.enabled_flag                  = 'Y'
            AND segments.end_user_column_name          = 'Related Items';

            EXCEPTION
            WHEN OTHERS THEN
            lv_column_name := NULL;
            END;

            IF lv_column_name IS NOT NULL 
            THEN
                EXECUTE IMMEDIATE 'SELECT '||lv_column_name||' FROM MTL_RELATED_ITEMS WHERE inventory_item_id = :1
                AND RELATED_ITEM_ID = :2 AND ORGANIZATION_ID = :3 AND RELATIONSHIP_TYPE_ID = :4'
                INTO lv_attribute_value
                USING p_in_inventory_item_id,p_in_related_item_id,p_in_org_id,p_in_relationship_type_id;
                
            END IF;
        EXCEPTION
        WHEN OTHERS THEN
            lv_attribute_value := NULL;
        END;
       
        RETURN lv_attribute_value;
    END xxpso_get_relation_addnt_attr;
    
    
    
    PROCEDURE Log_Msg (p_msg IN VARCHAR2)
    IS
        lc_msg  VARCHAR2 (4000) := p_msg;
    BEGIN
        IF gc_debug_flag = 'Y'
        THEN
            fnd_file.put_line (fnd_file.log, lc_msg);
            dbms_output.put_line (lc_msg);
        END IF; 
    EXCEPTION WHEN OTHERS 
    THEN
        lc_msg := 'Unhandled exception in Log_Msg. Error: '||SQLCODE||'->'||SQLERRM;
        fnd_file.put_line (fnd_file.log, lc_msg);
        dbms_output.put_line (lc_msg);
    END Log_Msg;
    
    
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
    


   /* ************************************************************************************
   * Function: add_text_node_fnc
   * Synopsis: This function will add text node to a node
   * PARAMETERS:
   *        p_in_doc
   *        p_in_parent_node
   *        p_in_node_name
   *        p_in_node_data   
   *
   * Return Values:
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            10-Mar-2016
   ************************************************************************************* */          
    FUNCTION add_text_node_fnc( p_in_doc xmldom.domdocument,
                                p_in_parent_node xmldom.domnode,
                                p_in_node_name VARCHAR2,
                                p_in_node_data VARCHAR2 
                                )
        RETURN xmldom.domnode
    IS          
        v_item_elmt xmldom.domelement;
        v_item_node xmldom.domnode;
        v_item_text xmldom.domtext;        
    BEGIN    
        v_item_elmt := xmldom.createelement (p_in_doc, p_in_node_name);
        v_item_node := xmldom.appendchild (p_in_parent_node, xmldom.makenode (v_item_elmt));
        v_item_text := xmldom.createtextnode (p_in_doc, p_in_node_data);
        v_item_node := xmldom.appendchild (v_item_node, xmldom.makenode (v_item_text));
        
        RETURN v_item_node;        
    END add_text_node_fnc;


   /* ************************************************************************************
   * Function: create_node_fnc
   * Synopsis: This function will create a node
   * PARAMETERS:
   *        p_in_doc
   *        p_in_node
   *        p_in_attr_nm
   *        p_in_attr_desc   
   *        p_in_attr_val
   *
   * Return Values:
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            10-Mar-2016
   ************************************************************************************* */         
    FUNCTION create_node_fnc(   p_in_doc        xmldom.domdocument,
                                p_in_node       xmldom.domnode,
                                p_in_attr_nm    VARCHAR2,
                                p_in_attr_desc  VARCHAR2 := NULL,
                                p_in_attr_val   VARCHAR2 := NULL 
                            )
        RETURN xmldom.domnode
    IS    
      v_new_elmt xmldom.domelement;
      v_user_node xmldom.domnode;      
    BEGIN    
      v_new_elmt  := xmldom.createelement (p_in_doc, p_in_attr_nm);
      v_user_node := xmldom.appendchild (p_in_node, xmldom.makenode (v_new_elmt));
      RETURN v_user_node;      
    END create_node_fnc;    
    
   /* ************************************************************************************
   * Function: get_related_item_dff_node
   *
   * Synopsis: This function will return dff node for related items
   *
   * PARAMETERS:
   *        p_in_message
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Akshay Nayak       1.0                                            21-Sep-2016
   ************************************************************************************* */  
    FUNCTION get_related_item_dff_node(p_in_attr_val IN VARCHAR2
                          ,p_in_attr_index    IN VARCHAR2
                          ,v_doc         IN xmldom.domdocument    
                          ,v_dff_node    IN xmldom.domnode
                          )
        RETURN xmldom.domnode
    IS
        v_item_node xmldom.domnode;
    BEGIN
    IF p_in_attr_val IS NOT NULL THEN
        v_item_node     := add_text_node_fnc (v_doc, v_dff_node, 'AttributeName','ATTR_CHAR'||p_in_attr_index );
        v_item_node     := add_text_node_fnc (v_doc, v_dff_node, 'AttributeValue', p_in_attr_val );
    END IF;  
    RETURN v_item_node; 
    END;    
   
    
   /* ************************************************************************************
   * Function: get_xml_error
   *
   * Synopsis: This function will create error in XML format
   *
   * PARAMETERS:
   *        p_in_message
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            24-Jun-2016
   ************************************************************************************* */  
    FUNCTION get_xml_error(p_in_message IN VARCHAR2)
        RETURN CLOB
    IS
        v_err_msg VARCHAR2 (2000) := p_in_message;
        v_clob CLOB               := '';
        v_doc xmldom.domdocument;
        v_child_node xmldom.domnode;
        v_main_node xmldom.domnode;
        v_root_node xmldom.domnode;
        v_user_node xmldom.domnode;
        v_item_node xmldom.domnode;
        v_root_elmt xmldom.domelement;
        v_item_elmt xmldom.domelement;
        v_item_text xmldom.domtext;
        v_entity_name VARCHAR2 (20);
        v_error_msg   VARCHAR2 (2000);
        v_search_cnt  NUMBER := 0;
    BEGIN
        DBMS_SESSION.free_unused_user_memory;
        -- instantiate new DOM document
        v_doc := xmldom.newdomdocument;
        
        -- create root element
        v_main_node := xmldom.makenode (v_doc);
        v_root_elmt := xmldom.createelement (v_doc, 'ORACLEMESSAGE');
        xmldom.setattribute (v_root_elmt, 'xmlns', 'http://pearson.com');
        v_root_node := xmldom.appendchild (v_main_node, xmldom.makenode (v_root_elmt));
        v_user_node := create_node_fnc (v_doc, v_root_node, 'PAYLOAD');
        v_item_node := add_text_node_fnc (v_doc, v_user_node, 'ERROR_MESSAGE', v_err_msg);
        DBMS_LOB.createtemporary (v_clob, TRUE);
        xmldom.writetoclob (v_doc, v_clob);
        xmldom.freedocument (v_doc);
        --Log_Msg (v_clob);
        
        RETURN v_clob;
        
    EXCEPTION
    WHEN OTHERS THEN
        v_error_msg := SUBSTR (SQLERRM, 1, 1000);
        Log_Msg (v_error_msg);
        RETURN v_error_msg;
    END get_xml_error;
    
    

   /* ************************************************************************************
   * Function: get_xml_message
   * Synopsis: This function will write output data in XML format
   * PARAMETERS:
   *        p_record_id
   *
   * Return Values:
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            10-Mar-2016
   |  Akshay Nayak         | 26-Sep-2016   | 1.2      | Integration related changes    
   ************************************************************************************* */  
    --Changes for v1.2
    --FUNCTION get_xml_message(p_record_id NUMBER)
    FUNCTION get_xml_message(p_record_id NUMBER
                    ,p_in_entity_name IN VARCHAR2
                    )
        RETURN CLOB
    IS
        v_clob          CLOB := '';
        v_doc           xmldom.domdocument;
        v_node          xmldom.domnode;
        v_child_node    xmldom.domnode;
        v_child_node1   xmldom.domnode;
        v_main_node     xmldom.domnode;
        v_root_node     xmldom.domnode;
        v_user_node     xmldom.domnode;    
        v_user_node1    xmldom.domnode;
        v_user_node2    xmldom.domnode;
        v_user_node3    xmldom.domnode;
        v_user_node4    xmldom.domnode;
        v_user_node5    xmldom.domnode;
        v_user_node6    xmldom.domnode;
        v_user_node7    xmldom.domnode;
        v_user_node8    xmldom.domnode;
        v_user_node9    xmldom.domnode;
        v_user_node10   xmldom.domnode;
        v_user_node11   xmldom.domnode;
        v_user_node12   xmldom.domnode;
        v_user_node13   xmldom.domnode;
        v_user_node14   xmldom.domnode;
        v_user_node15   xmldom.domnode;
        v_user_node31   xmldom.domnode;
        v_item_node     xmldom.domnode;    
        v_root_elmt     xmldom.domelement;
        v_item_elmt     xmldom.domelement;
        v_v_item_text   xmldom.domtext;
        v_entity_name   VARCHAR2 (20);
        v_error_msg     VARCHAR2 (2000);
        v_attr_value    VARCHAR2 (1000);
        l_count         NUMBER  := 0;
        
        --Changes for GHEPMv2
        v_dff_node        xmldom.domnode; 
        v_item_cat_node     xmldom.domnode;        
        
        -- Item Master Cursor 
        CURSOR c_item_cur
        IS
             SELECT  msib.inventory_item_id             inventory_item_id
                    ,msib.segment1                      item_number          
                    ,msib.organization_id               organization_id
                    ,mp.organization_code                 organization_code
                    ,hou.name                              organization_name
                    ,cat.concatenated_segments          item_catalog_group
                    ,mstl.description                      item_description
                    ,mstl.long_description              long_description
                    ,msib.primary_unit_of_measure       primary_unit_of_measure
                    ,msib.inventory_item_status_code    inventory_item_status_code
                    --,msib.item_type                     item_type
                    ,apps.INV_MEANING_SEL.c_fndcommon(msib.ITEM_TYPE,'ITEM_TYPE')
                                                        item_type
                    ,pel.name                           lifecycle
                    ,pelp.name                          current_phase
                    ,msib.approval_status               approval_status
                    ,fu1.user_name                      created_by_name
                    ,msib.creation_date                 creation_date        
                    ,fu2.user_name                      last_updated_by_name
                    ,msib.last_update_date              last_update_date
               FROM apps.xxpso_pdh_outqueue_items_stg   stg,
                    apps.mtl_system_items_b             msib,
                    apps.mtl_system_items_tl            mstl,
                    apps.mtl_parameters                 mp,
                    apps.hr_organization_units          hou,    
                    mtl_item_catalog_groups_kfv         cat,
                    apps.pa_ego_lifecycles_v            pel,
                    apps.pa_ego_lifecycles_phases_v     pelp,
                    apps.fnd_user                       fu1,
                    apps.fnd_user                       fu2
              WHERE stg.inventory_item_id               = msib.inventory_item_id
                AND stg.organization_id                 = msib.organization_id
                AND msib.inventory_item_id              = mstl.inventory_item_id
                AND msib.organization_id                = mstl.organization_id
                AND msib.enabled_flag                   = 'Y'
                AND msib.organization_id                = mp.organization_id
                AND mp.organization_id                  = hou.organization_id
                AND msib.item_catalog_group_id          = cat.item_catalog_group_id
                AND msib.lifecycle_id                   = pel.proj_element_id(+)
                AND pel.object_type(+)                  = 'PA_STRUCTURES' 
                AND msib.lifecycle_id                   = pelp.parent_structure_id(+)
                AND msib.current_phase_id               = pelp.proj_element_id(+)
                AND pelp.object_type(+)                 = 'PA_TASKS'
                AND msib.created_by                     = fu1.user_id(+)
                AND msib.last_updated_by                = fu2.user_id(+)
                AND stg.record_id                       = p_record_id
           ORDER BY msib.inventory_item_id;
                
        
        -- Item Cross Reference Cursor
        CURSOR c_xref_cur (p_item_id NUMBER)
        IS       
             SELECT mcr.inventory_item_id, 
                    mcr.cross_reference_type, 
                    mcr.cross_reference, 
                    mcr.description, 
                    hosv.orig_system,
                    mcr.start_date_active,
                    mcr.end_date_active
               FROM mtl_cross_references                mcr, 
                    hz_orig_systems_vl                  hosv
              WHERE mcr.source_system_id                = hosv.orig_system_id
                AND mcr.cross_reference_type            = 'SS_ITEM_XREF'
                AND TRUNC(SYSDATE)  BETWEEN TRUNC(NVL(mcr.start_date_active,SYSDATE-1)) 
                    AND TRUNC(NVL(mcr.end_date_active,SYSDATE+1))
                AND TRUNC(SYSDATE)  BETWEEN TRUNC(NVL(hosv.start_date_active,SYSDATE-1)) 
                    AND TRUNC(NVL(hosv.end_date_active,SYSDATE+1))
                AND hosv.status                         = 'A'
                AND mcr.inventory_item_id               = p_item_id;
                
                
        -- UDA Cursor 1   
        CURSOR c_attr_grp_cur (p_item_id NUMBER)
        IS
             SELECT DISTINCT 
                    atg.attr_group_type, 
                    atg.attr_group_id, 
                    atg.attr_group_name, 
                    atg.attr_group_disp_name, 
                    atg.multi_row_code
               FROM apps.ego_mtl_sy_items_ext_vl         ego,
                    apps.xxpso_ego_attr_groups_v        atg
              WHERE ego.attr_group_id                   = atg.attr_group_id
                AND atg.application_id                  = 431
                AND ego.inventory_item_id               = p_item_id
           ORDER BY atg.attr_group_name;               
           
        -- UDA Cursor 2
        CURSOR c_attr_grp_cur2 (p_item_id NUMBER, p_attr_group_id NUMBER)
        IS
             SELECT ego.extension_id
               FROM apps.ego_mtl_sy_items_ext_vl         ego
              WHERE ego.inventory_item_id               = p_item_id
                AND ego.attr_group_id                   = p_attr_group_id
           ORDER BY ego.extension_id;   
           
        -- UDA Cursor 3    
        CURSOR c_attr_cur (p_extension_id NUMBER)
        IS
             SELECT ego.inventory_item_id, 
                    ego.extension_id, 
                    att.attr_name, 
                    att.attr_display_name, 
                    att.database_column, 
                    att.data_type_code
               FROM apps.ego_mtl_sy_items_ext_vl         ego,
                    apps.xxpso_ego_attr_groups_v        atg,
                    apps.xxpso_ego_attrs_v              att
              WHERE ego.attr_group_id                   = atg.attr_group_id
                AND atg.attr_group_type                 = att.attr_group_type
                AND atg.attr_group_name                 = att.attr_group_name
                AND atg.application_id                  = 431
                AND att.application_id                  = 431
                AND att.enabled_flag                    = 'Y'
                AND ego.extension_id                    = p_extension_id
           ORDER BY ego.extension_id, att.attr_name;

                
        -- Added this cursor on 08-June-2015
        -- Item Revision cursor 
        CURSOR c_item_rev_cur (p_item_id NUMBER, p_org_id NUMBER)
        IS  
             SELECT revision,
                    revision_label,
                    revision_reason,
                    description,
                    effectivity_date,
                    implementation_date,
                    change_notice,
                    ecn_initiation_date
               FROM apps.mtl_item_revisions_vl          
              WHERE inventory_item_id                   = p_item_id       
                AND organization_id                     = p_org_id
           ORDER BY revision;
    
                
        -- Added this cursor on 08-June-2015        
        CURSOR c_rel_items_cur (p_item_id NUMBER, p_org_id NUMBER)
        IS  
             SELECT mri.related_item_id                 related_item_id, 
                    msib.segment1                       related_item_number,
                    mri.relationship_type_id            relationship_type_id,
                    lkp.meaning                         relationship_type, 
                    mri.reciprocal_flag                 reciprocal_flag,
                    mri.planning_enabled_flag           planning_enabled_flag,
                    --mri.start_date                      start_date,
                    --mri.end_date                        end_date,
                   -- TO_DATE(mri.ATTR_DATE1,'YYYY/MM/DD HH24:MI:SS') start_date,
                   --TO_DATE(mri.ATTR_DATE2,'YYYY/MM/DD HH24:MI:SS') end_date,
                    dff.start_date           start_date, --Changes to 1.3
                    dff.end_date             end_date,--Changes to 1.3
                    dff.attr_char1            attr_char1,
                    dff.attr_char2            attr_char2,
                    dff.attr_char3            attr_char3,
                    dff.attr_char4            attr_char4,
                    dff.attr_char5            attr_char5,
                    dff.attr_char6            attr_char6,
                    dff.attr_char7            attr_char7,
                    dff.attr_char8            attr_char8,
                    dff.attr_char9            attr_char9,
                    dff.attr_char10            attr_char10,
                    dff.attr_num1            attr_num1,
                    dff.attr_num2            attr_num2,
                    dff.attr_num3            attr_num3,
                    dff.attr_num4            attr_num4,
                    dff.attr_num5            attr_num5,
                    dff.attr_num6            attr_num6,
                    dff.attr_num7            attr_num7,
                    dff.attr_num8            attr_num8,
                    dff.attr_num9            attr_num9,
                    dff.attr_num10            attr_num10,
                    dff.attr_date1            attr_date1,
                    dff.attr_date2            attr_date2,
                    dff.attr_date3            attr_date3,
                    dff.attr_date4            attr_date4,
                    dff.attr_date5            attr_date5,
                    dff.attr_date6            attr_date6,
                    dff.attr_date7            attr_date7,
                    dff.attr_date8            attr_date8,
                    dff.attr_date9            attr_date9,
                    dff.attr_date10            attr_date10
               FROM apps.mtl_related_items              mri,
                    apps.mtl_system_items_b             msib, 
                    apps.xxpso_ego_related_itms_dff     dff,
                    mfg_lookups                         lkp
              WHERE mri.related_item_id                 = msib.inventory_item_id--Changes to 1.3
                AND mri.organization_id                 = msib.organization_id
                AND mri.inventory_item_id               = dff.inventory_item_id(+)
                AND mri.organization_id                 = dff.organization_id(+)
                AND mri.related_item_id                 = dff.related_item_id(+)
                AND mri.relationship_type_id            = dff.relation_type(+)
                AND lkp.lookup_type                     = 'MTL_RELATIONSHIP_TYPES'
                AND lkp.enabled_flag                    = 'Y'
                AND TRUNC(SYSDATE)  BETWEEN TRUNC(NVL(lkp.start_date_active,SYSDATE-1)) 
                    AND TRUNC(NVL(lkp.end_date_active,SYSDATE+1))
                -- AND TRUNC(SYSDATE)  BETWEEN TRUNC(NVL(mri.start_date,SYSDATE-1)) 
                --     AND TRUNC(NVL(mri.end_date,SYSDATE+1))
                --AND TRUNC(SYSDATE)  BETWEEN TRUNC(NVL( TO_DATE(mri.ATTR_DATE1,'YYYY/MM/DD HH24:MI:SS'),SYSDATE-1)) 
                --    AND TRUNC(NVL(TO_DATE(mri.ATTR_DATE2,'YYYY/MM/DD HH24:MI:SS'),SYSDATE+1))    
                AND ( --(mri.end_date IS NULL  OR mri.end_date > SYSDATE )  -- changes for 1.4 
            	  --AND  -- changes for 1.4
                (dff.end_date IS NULL  OR dff.end_date > SYSDATE )      
            	)             
                AND lkp.lookup_code                     = TO_CHAR(mri.relationship_type_id)
                AND mri.inventory_item_id               = p_item_id       
                AND mri.organization_id                 = p_org_id
           ORDER BY mri.inventory_item_id, mri.related_item_id, mri.relationship_type_id;
               
               
        -- Added this cursor on 08-June-2015             
        CURSOR c_struct_cur (p_item_id NUMBER, p_org_id NUMBER)
        IS  
            SELECT bs.bill_sequence_id                          bill_sequence_id,
                   msib.segment1                                Assembly_Item_Number,
                   msib.inventory_item_id                       assembly_item_id,
                   msib.organization_id                         organization_id,
                   bst.structure_type_name                      structure_type_name,
                   NVL (bs.alternate_bom_designator, 'Primary') structure_name,
                   mp.organization_code                         organization_code,
                   hou.name                                     organization_name,
                   DECODE(bs.assembly_type, 1, 
                   'Manufacturing Bill', 'Engineering Bill')    assembly_type                   
              FROM apps.bom_structures_b                bs,
                   apps.mtl_system_items_b              msib,
                   apps.mtl_parameters                  mp,
                   apps.hr_organization_units           hou,    
                   apps.bom_structure_types_b           bst
             WHERE bs.assembly_item_id                  = msib.inventory_item_id
               AND bs.organization_id                   = msib.organization_id
               AND msib.organization_id                 = mp.organization_id
               AND mp.organization_id                   = hou.organization_id
               AND bs.structure_type_id                 = bst.structure_type_id
               AND msib.inventory_item_id               = p_item_id  
               AND msib.organization_id                 = p_org_id;
               
               
        -- Added this cursor on 08-June-2015           
        CURSOR c_comp_cur (p_bill_sequence_id NUMBER)
        IS   
          SELECT ms.segment1   component_number,
                 bs.component_quantity,
                 bs.operation_seq_num,
                 bs.item_num,
                 bs.planning_factor,                
                 bs.effectivity_date,
                 bs.disable_date
            FROM apps.bom_components_b                  bs, 
                 apps.mtl_system_items_b                ms
           WHERE bs.component_item_id                   = ms.inventory_item_id
             AND bs.bill_sequence_id                    = p_bill_sequence_id
             -- AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(bs.effectivity_date,SYSDATE-1)) 
             --     AND TRUNC(NVL(bs.disable_date,SYSDATE+1))
             AND (bs.disable_date IS NULL OR bs.disable_date > SYSDATE)
        ORDER BY item_num;
        
        --Changes for GHEPMv2
        CURSOR c_item_cat_cur (p_item_id NUMBER)
        IS         
            SELECT msib.inventory_item_id   inventory_item_id
              ,msib.segment1                item_number 
              ,mp.organization_code         organization_code 
              ,hou.name                     organization_name 
              ,mcs.category_set_name        category_set_name
              ,mic.category_id              category_id
              ,catkfv.concatenated_segments category_name
              ,fu1.user_name                created_by_name
              ,mic.creation_date            creation_date        
              ,fu2.user_name                last_updated_by_name
              ,mic.last_update_date         last_update_date              
            FROM apps.mtl_system_items_b msib 
              ,apps.mtl_parameters mp 
              ,apps.hr_organization_units hou 
              ,apps.mtl_item_categories mic 
              ,apps.mtl_category_sets mcs
              ,apps.mtl_categories_b_kfv catkfv
              ,apps.fnd_user                      fu1
              ,apps.fnd_user                      fu2              
            WHERE msib.organization_id      = mp.organization_id
            AND mp.organization_id          = hou.organization_id
            AND mic.inventory_item_id       = p_item_id
            AND mic.inventory_item_id       = msib.inventory_item_id
            AND mic.organization_id         = msib.organization_id
            AND mic.category_set_id         = mcs.category_set_id(+)
            AND mic.category_id             = catkfv.category_id
            AND mcs.structure_id(+)         = catkfv.structure_id
            AND mic.created_by              = fu1.user_id 
            AND mic.last_updated_by         = fu2.user_id 
       ORDER BY msib.inventory_item_id; 
            
        CURSOR c_item_attached_doc (p_item_id NUMBER)
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
          and msib.inventory_item_id = p_item_id
          order by msib.inventory_item_id;                
            

       l_rec_items_rec            xxpso_ego_items_stg%ROWTYPE;
       l_rec_bom_assembly_rec        xxpso_ego_bom_assembly_stg%ROWTYPE;
       l_rec_related_items_rec        xxpso_ego_related_itms_stg%ROWTYPE;
       lv_rel_item_record_found        VARCHAR2(1);    
       lv_bom_record_found        VARCHAR2(1);
       lv_item_record_found        VARCHAR2(1);
       lv_originating_system        VARCHAR2(10);
       lv_originating_system_ref     VARCHAR2(255);
       lv_originating_system_rel_ref     VARCHAR2(255);
       lv_originating_system_rel_typ    NUMBER;
       lv_originating_system_date     DATE;
       lv_transaction_type        VARCHAR2(10);
       v_header_node            xmldom.domnode;    
       
       ln_related_item_id        NUMBER;
       ln_relationship_type_id        NUMBER;
       
       --Changes for v1.6	Begin
       lv_mdm_transaction_type		VARCHAR2(10);
       ld_out_rel_last_date		DATE;
       
            
    BEGIN
    
        Log_Msg ('In get_xml_message procedure');

        DBMS_SESSION.free_unused_user_memory;

        -- instantiate new DOM document
        v_doc := xmldom.newdomdocument;

        -- create root element
        v_main_node := xmldom.makenode (v_doc);

        -- Item Loop
        FOR c_item_rec IN c_item_cur
        LOOP
        
            v_user_node1    := create_node_fnc (v_doc, v_main_node, 'ItemType'); 
            
            --Changes for v1.6
            BEGIN
            SELECT transaction_type
              INTO lv_mdm_transaction_type
              FROM xxpso_pdh_outqueue_items_stg
             WHERE record_id = p_record_id;
            EXCEPTION
            WHEN OTHERS THEN
            	lv_mdm_transaction_type := 'NULL';
            END;
            
                --Get the last updated record.
                IF p_in_entity_name = gv_item_entity THEN
            BEGIN
        Log_Msg('Fetching Integration item information for inventory_item_id:'||c_item_rec.inventory_item_id||
                ' organization_id:'||c_item_rec.organization_id);            
            SELECT * 
              INTO l_rec_items_rec
              FROM xxpso_ego_items_stg stg1
             WHERE stg1.inventory_item_id = c_item_rec.inventory_item_id
               AND stg1.organization_id = c_item_rec.organization_id
               AND stg1.status_code = gv_processed_flag
               AND stg1.published_by_esb = 'N'
               AND stg1.last_update_date = (select max(stg2.last_update_date)
                            FROM  xxpso_ego_items_stg stg2
                            WHERE stg2.inventory_item_id = stg1.inventory_item_id
                              AND stg2.organization_id = stg1.organization_id
                              AND stg2.status_code = gv_processed_flag
                              AND stg2.published_by_esb = 'N'                          
                           );

            --After getting the last updated record update     published_by_esb flag to Y for all earlier and exising records.           
            UPDATE xxpso_ego_items_stg
               SET published_by_esb = 'Y'
             WHERE inventory_item_id = c_item_rec.inventory_item_id
               AND organization_id = c_item_rec.organization_id
               AND status_code = gv_processed_flag
               AND published_by_esb = 'N';

            Log_Msg('Item record found: '||l_rec_items_rec.record_id);
            lv_item_record_found := 'Y';

            EXCEPTION
            WHEN OTHERS THEN
            lv_item_record_found := 'N';
            END;

            IF lv_item_record_found = 'Y' THEN
                lv_originating_system := l_rec_items_rec.source_system;
                lv_originating_system_ref := l_rec_items_rec.source_item_number;
                lv_originating_system_date:= l_rec_items_rec.last_update_date;
                    IF l_rec_items_rec.transaction_type = 'CREATE' THEN
                    	lv_transaction_type       := 'C';
                    ELSIF l_rec_items_rec.transaction_type = 'UPDATE' THEN
                    	lv_transaction_type       := 'U';
                    END IF;
                    --lv_transaction_type       := l_rec_items_rec.transaction_type;
                Log_Msg('Record Id of matched Item:'||l_rec_items_rec.record_id);
            ELSE
                lv_originating_system := 'MDM';
                lv_originating_system_date := c_item_rec.last_update_date;
	        lv_originating_system_ref  := NULL;--Changes for v1.4
	        --lv_transaction_type	       := NULL;--Changes for v1.4   
	        lv_transaction_type	       := lv_mdm_transaction_type; --Changes for v1.5
            END IF;  
            Log_Msg('Item record found flag:'||lv_item_record_found);
            v_header_node   := create_node_fnc (v_doc, v_user_node1, 'Header');
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'OrigSystem', lv_originating_system );
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'OrigSystemReference', lv_originating_system_ref );
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'OrigSystemTimeStamp', 
            TO_CHAR(lv_originating_system_date,'YYYY-MM-DD')||'T'||TO_CHAR(lv_originating_system_date,'HH24:MI:SS')||'Z' );
            --to_char(lv_originating_system_date,'DD-MM-YYYY HH24:MI:SS') );
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'MDMReference', c_item_rec.inventory_item_id );
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'RecordType', lv_transaction_type  );    
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'Entity', p_in_entity_name );    
        END IF;
                
            
            --v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'ItemId', c_item_rec.inventory_item_id );

            
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'OrigSystem', 'MDM' );
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'ItemNumber', c_item_rec.item_number );
            --v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'OrganizationID', c_item_rec.organization_id );
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'OrganizationCode', c_item_rec.organization_code );
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'OrganizationName', c_item_rec.organization_name );
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'CatalogCategory', c_item_rec.item_catalog_group );
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'Description', c_item_rec.item_description );    
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'LongDescription', c_item_rec.long_description );
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'PrimaryUnitOfMeasure', c_item_rec.primary_unit_of_measure );
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'ItemStatus', c_item_rec.inventory_item_status_code );
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'UserItemType', c_item_rec.item_type );   
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'LifeCycle', c_item_rec.lifecycle );    
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'LifeCyclePhase', c_item_rec.current_phase );
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'ApprovalStatus', c_item_rec.approval_status );
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'CreationDate', TO_CHAR(c_item_rec.creation_date,'YYYY-MM-DD')||'T'||TO_CHAR(c_item_rec.creation_date,'HH24:MI:SS')||'Z' );
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'CreatedBy', c_item_rec.created_by_name );
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'LastUpdateDate', TO_CHAR(c_item_rec.last_update_date,'YYYY-MM-DD')||'T'||TO_CHAR(c_item_rec.last_update_date,'HH24:MI:SS')||'Z' );        
            v_item_node     := add_text_node_fnc (v_doc, v_user_node1, 'LastUpdatedBy', c_item_rec.last_updated_by_name );      
        
        
            -- Cross Ref Loop
            FOR c_xref_rec IN c_xref_cur (c_item_rec.inventory_item_id)
            LOOP
                v_user_node5     := create_node_fnc (v_doc, v_user_node1, 'CrossReferences');
                v_item_node     := add_text_node_fnc (v_doc, v_user_node5, 'CrossReferenceType', c_xref_rec.cross_reference_type );    
                v_item_node     := add_text_node_fnc (v_doc, v_user_node5, 'CrossReference', c_xref_rec.cross_reference );
                v_item_node     := add_text_node_fnc (v_doc, v_user_node5, 'Description', c_xref_rec.description );
                v_item_node     := add_text_node_fnc (v_doc, v_user_node5, 'SourceSystem', c_xref_rec.orig_system );
                IF c_xref_rec.start_date_active IS NOT NULL THEN
                    v_item_node     := add_text_node_fnc (v_doc, v_user_node5, 'StartDate', TO_CHAR(c_xref_rec.start_date_active,'YYYY-MM-DD')||'T'||TO_CHAR(c_xref_rec.start_date_active,'HH24:MI:SS')||'Z' ); 
                END IF; 
                IF c_xref_rec.end_date_active IS NOT NULL THEN
                    v_item_node     := add_text_node_fnc (v_doc, v_user_node5, 'StartDate', TO_CHAR(c_xref_rec.end_date_active,'YYYY-MM-DD')||'T'||TO_CHAR(c_xref_rec.end_date_active,'HH24:MI:SS')||'Z' );         
                END IF;
            END LOOP;    
            
            
            v_user_node2     := create_node_fnc (v_doc, v_user_node1, 'UserAttributes');
       
            -- UDA Attribute Group Loop
            FOR c_attr_grp_rec IN c_attr_grp_cur (c_item_rec.inventory_item_id)
            LOOP                    
                v_user_node3    := create_node_fnc (v_doc, v_user_node2, 'UserAttributeGroup');
                v_item_node     := add_text_node_fnc (v_doc, v_user_node3, 'AttrGroupInternalName', c_attr_grp_rec.attr_group_name );
                v_item_node     := add_text_node_fnc (v_doc, v_user_node3, 'AttrGroupDisplayName', c_attr_grp_rec.attr_group_disp_name );
                v_item_node     := add_text_node_fnc (v_doc, v_user_node3, 'MultiRowFlag', c_attr_grp_rec.multi_row_code );
                                        
                FOR c_attr_grp_rec2 IN c_attr_grp_cur2 (c_item_rec.inventory_item_id, c_attr_grp_rec.attr_group_id)
                LOOP
                    v_user_node31     := create_node_fnc (v_doc, v_user_node3, 'UserAttributes');
                    
                    -- UDA Attribute Loop        
                    FOR c_attr_rec IN c_attr_cur (c_attr_grp_rec2.extension_id)
                    LOOP
                        EXECUTE IMMEDIATE  'SELECT TO_CHAR( '|| c_attr_rec.database_column ||' ) FROM ego_mtl_sy_items_ext_vl where inventory_item_id = '
                                            || c_attr_rec.inventory_item_id || ' AND extension_id = ' || c_attr_rec.extension_id || ''
                        INTO v_attr_value;                        
                        
                        IF v_attr_value IS NOT NULL
                        THEN
                            v_user_node4     := create_node_fnc (v_doc, v_user_node31, 'UserAttribute');
                            v_item_node     := add_text_node_fnc (v_doc, v_user_node4, 'InternalName', c_attr_rec.attr_name );
                            v_item_node     := add_text_node_fnc (v_doc, v_user_node4, 'DisplayName', c_attr_rec.attr_display_name );
                            v_item_node     := add_text_node_fnc (v_doc, v_user_node4, 'DataType', c_attr_rec.data_type_code );
                            v_item_node     := add_text_node_fnc (v_doc, v_user_node4, 'GroupIdentifier', c_attr_rec.extension_id );
                            v_item_node     := add_text_node_fnc (v_doc, v_user_node4, 'Value', v_attr_value );    
                        END IF;      
                        
                    END LOOP;  -- c_attr_rec Loop
                END LOOP;  -- c_attr_grp_rec2 Loop
            END LOOP;  -- c_attr_grp_rec Loop
            
            
            -- Added this block on 08-June-2015
            -- Item Revision Loop
            FOR c_item_rev_rec IN c_item_rev_cur (c_item_rec.inventory_item_id, c_item_rec.organization_id)
            LOOP
                v_user_node13   := create_node_fnc (v_doc, v_user_node1, 'RevisionHeader');
                
                v_item_node     := add_text_node_fnc (v_doc, v_user_node13, 'Revision', c_item_rev_rec.revision );    
                v_item_node     := add_text_node_fnc (v_doc, v_user_node13, 'RevisionLabel', c_item_rev_rec.revision_label );
                v_item_node     := add_text_node_fnc (v_doc, v_user_node13, 'RevisionReason', c_item_rev_rec.revision_reason );
                v_item_node     := add_text_node_fnc (v_doc, v_user_node13, 'RevisionDescription', c_item_rev_rec.description );  
                IF c_item_rev_rec.effectivity_date IS NOT NULL THEN
                    v_item_node     := add_text_node_fnc (v_doc, v_user_node13, 'RevisionEffectiveDate', TO_CHAR(c_item_rev_rec.effectivity_date,'YYYY-MM-DD')||'T'||TO_CHAR(c_item_rev_rec.effectivity_date,'HH24:MI:SS')||'Z' );
                END IF; 
                --v_item_node     := add_text_node_fnc (v_doc, v_user_node13, 'RevisionImplementationDate', TO_CHAR(c_item_rev_rec.implementation_date,'YYYY-MM-DD')||'T'||TO_CHAR(c_item_rev_rec.implementation_date,'HH24:MI:SS')||'Z' );  
            END LOOP;                    
            
            
            -- Added this block on 08-June-2015
            -- Related Items Loop
        IF p_in_entity_name = gv_related_item_entity THEN
            BEGIN
            
            select related_item_id , relationship_type_id,last_update_date
              INTO ln_related_item_id,ln_relationship_type_id,ld_out_rel_last_date
              FROM xxpso_pdh_outqueue_items_stg out_stg
              WHERE out_stg.record_id = p_record_id;

            Log_Msg('Fetching Integration Related item information for inventory_item_id:'||c_item_rec.inventory_item_id||
                ' related_item_id:'||ln_related_item_id ||
                ' relationship_type_id:'||ln_relationship_type_id ||
                ' organization_id:'||c_item_rec.organization_id
                );    

            SELECT * 
              INTO l_rec_related_items_rec
              FROM xxpso_ego_related_itms_stg rel_stg1
             WHERE rel_stg1.inventory_item_id = c_item_rec.inventory_item_id
               AND rel_stg1.related_item_id = ln_related_item_id
               AND rel_stg1.organization_id = c_item_rec.organization_id
               AND rel_stg1.relation_type   = ln_relationship_type_id
               AND rel_stg1.status_code = gv_processed_flag
               AND rel_stg1.published_by_esb = 'N'
               AND rel_stg1.last_update_date = (select max(rel_stg2.last_update_date)
                            FROM  xxpso_ego_related_itms_stg rel_stg2
                            WHERE rel_stg2.inventory_item_id = rel_stg1.inventory_item_id
                              AND rel_stg2.related_item_id = rel_stg1.related_item_id
                              AND rel_stg2.organization_id = rel_stg1.organization_id
                              AND rel_stg2.relation_type = rel_stg1.relation_type
                              AND rel_stg2.status_code = gv_processed_flag
                              AND rel_stg2.published_by_esb = 'N'                          
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
                lv_originating_system_rel_ref := l_rec_related_items_rec.source_sys_rel_itm_num;
                lv_originating_system_rel_typ := l_rec_related_items_rec.relation_type;
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
                lv_originating_system_date := ld_out_rel_last_date;
                    lv_originating_system_ref  := NULL;--Changes for v1.5
                    --lv_transaction_type	       := NULL;--Changes for v1.5   
                    lv_transaction_type	       := lv_mdm_transaction_type; --Changes for v1.5
            END IF;
            Log_Msg('Related Item record found flag:'||lv_rel_item_record_found);
            v_header_node   := create_node_fnc (v_doc, v_user_node1, 'Header');
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'OrigSystem', lv_originating_system );
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'OrigSystemReference', lv_originating_system_ref );
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'OrigSystemTimeStamp', 
                                TO_CHAR(lv_originating_system_date,'YYYY-MM-DD')||'T'||TO_CHAR(lv_originating_system_date,'HH24:MI:SS')||'Z' );
            
            IF lv_rel_item_record_found = 'Y' THEN
		    v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'MDMReference', 
						l_rec_related_items_rec.inventory_item_id || '_' ||
						 l_rec_related_items_rec.related_item_id||'_' ||
						 l_rec_related_items_rec.relation_type||'_'||
						 l_rec_related_items_rec.organization_id);
	    ELSIF lv_rel_item_record_found = 'N' THEN       
	    	   v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'MDMReference', NULL);
	    END IF;
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'RecordType', lv_transaction_type  );    
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'Entity', p_in_entity_name );    
            
            
            --After getting the last updated record update     published_by_etl flag to Y for all earlier and exising records.           
            UPDATE xxpso_ego_related_itms_stg
               SET published_by_esb = 'Y'
             WHERE inventory_item_id = l_rec_related_items_rec.inventory_item_id
               AND related_item_id = l_rec_related_items_rec.related_item_id
               AND organization_id = l_rec_related_items_rec.organization_id
               AND relation_type = l_rec_related_items_rec.relation_type
               AND status_code = gv_processed_flag
               AND published_by_esb = 'N';              
            
        END IF;            
            FOR c_rel_items_rec IN c_rel_items_cur (c_item_rec.inventory_item_id, c_item_rec.organization_id)
            LOOP
                --Get the last updated record.

                v_user_node6     := create_node_fnc (v_doc, v_user_node1, 'RelatedItem');
                
                --v_item_node     := add_text_node_fnc (v_doc, v_user_node6, 'RelatedItemId', c_rel_items_rec.related_item_id );    
                v_item_node     := add_text_node_fnc (v_doc, v_user_node6, 'RelatedItemNumber', c_rel_items_rec.related_item_number );
                --v_item_node     := add_text_node_fnc (v_doc, v_user_node6, 'RelationshipType', c_rel_items_rec.relationship_type );
                v_item_node     := add_text_node_fnc (v_doc, v_user_node6, 'RelationshipType', c_rel_items_rec.relationship_type_id );                
                v_item_node     := add_text_node_fnc (v_doc, v_user_node6, 'ReciprocalFlag', c_rel_items_rec.reciprocal_flag );
                v_item_node     := add_text_node_fnc (v_doc, v_user_node6, 'PlanningEnabledFlag', c_rel_items_rec.planning_enabled_flag );
                IF c_rel_items_rec.start_date IS NOT NULL THEN 
                    v_item_node     := add_text_node_fnc (v_doc, v_user_node6, 'StartDate', TO_CHAR(c_rel_items_rec.start_date,'YYYY-MM-DD')||'T'||TO_CHAR(c_rel_items_rec.start_date,'HH24:MI:SS')||'Z' );
                END IF; 
                IF c_rel_items_rec.end_date IS NOT NULL THEN 
                    v_item_node     := add_text_node_fnc (v_doc, v_user_node6, 'EndDate', TO_CHAR(c_rel_items_rec.end_date,'YYYY-MM-DD')||'T'||TO_CHAR(c_rel_items_rec.end_date,'HH24:MI:SS')||'Z' );   
                END IF; 
                
                --Changes for GHEPMv2 Begin
                v_dff_node     := create_node_fnc (v_doc, v_user_node6, 'DFFAttributes');
               v_item_node    := get_related_item_dff_node(c_rel_items_rec.attr_char1,'1',v_doc,v_dff_node);
               v_item_node    := get_related_item_dff_node(c_rel_items_rec.attr_char2,'2',v_doc,v_dff_node);
               v_item_node    := get_related_item_dff_node(c_rel_items_rec.attr_char3,'3',v_doc,v_dff_node);
               v_item_node    := get_related_item_dff_node(c_rel_items_rec.attr_char4,'4',v_doc,v_dff_node);
               v_item_node    := get_related_item_dff_node(c_rel_items_rec.attr_char5,'5',v_doc,v_dff_node);
               v_item_node    := get_related_item_dff_node(c_rel_items_rec.attr_char6,'6',v_doc,v_dff_node);
               v_item_node    := get_related_item_dff_node(c_rel_items_rec.attr_char7,'7',v_doc,v_dff_node);
               v_item_node    := get_related_item_dff_node(c_rel_items_rec.attr_char8,'8',v_doc,v_dff_node);
               v_item_node    := get_related_item_dff_node(c_rel_items_rec.attr_char9,'9',v_doc,v_dff_node);
               v_item_node    := get_related_item_dff_node(c_rel_items_rec.attr_char10,'10',v_doc,v_dff_node);
                --Changes for GHEPMv2 End                
            END LOOP;         
            
            

            -- BOM Loop
            FOR c_struct_rec IN c_struct_cur (c_item_rec.inventory_item_id, c_item_rec.organization_id)
            LOOP
                --Get the last updated record.
                IF p_in_entity_name = gv_bom_entity THEN
            BEGIN
            Log_Msg('Fetching Integration BOM item information for assembly_item_id:'||c_struct_rec.assembly_item_id||
                ' structure_name:'||c_struct_rec.structure_name ||
                ' organization_id:'||c_struct_rec.organization_id);    
            SELECT * 
              INTO l_rec_bom_assembly_rec
              FROM xxpso_ego_bom_assembly_stg stg1
             WHERE assembly_item_id = c_struct_rec.assembly_item_id
               AND organization_id = c_struct_rec.organization_id
               AND structure_name = NVL(c_struct_rec.structure_name,'Primary')
               AND status_code = gv_processed_flag
               AND published_by_esb = 'N'
               AND last_update_date = (select max(last_update_date)
                            FROM  xxpso_ego_bom_assembly_stg stg2
                            WHERE stg2.assembly_item_id = stg1.assembly_item_id
                              AND stg2.organization_id = stg1.organization_id
                              AND stg2.structure_name = NVL(stg1.structure_name,'Primary')
                              AND stg2.status_code = gv_processed_flag
                              AND stg2.published_by_esb = 'N'                          
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
                    lv_originating_system_ref  := NULL;--Changes for v1.5
                    --lv_transaction_type	       := NULL;--Changes for v1.5                
                     lv_transaction_type	       := lv_mdm_transaction_type; --Changes for v1.6
            END IF;    
            Log_Msg('BOM record found flag:'||lv_bom_record_found);
            v_header_node   := create_node_fnc (v_doc, v_user_node1, 'Header');
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'OrigSystem', lv_originating_system );
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'OrigSystemReference', lv_originating_system_ref );
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'OrigSystemTimeStamp', 
                               TO_CHAR(lv_originating_system_date,'YYYY-MM-DD')||'T'||TO_CHAR(lv_originating_system_date,'HH24:MI:SS')||'Z' );
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'MDMReference', c_struct_rec.bill_sequence_id );
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'RecordType', lv_transaction_type  );    
            v_item_node     := add_text_node_fnc (v_doc, v_header_node, 'Entity', p_in_entity_name );
            
        --After getting the last updated record update     published_by_etl flag to Y for all earlier and exising records.           
        UPDATE xxpso_ego_bom_assembly_stg
           SET published_by_esb = 'Y'
         WHERE assembly_item_id = l_rec_bom_assembly_rec.assembly_item_id
           AND organization_id = l_rec_bom_assembly_rec.organization_id
                   AND structure_name = NVL(l_rec_bom_assembly_rec.structure_name,'Primary')
                   AND status_code = gv_processed_flag
                   AND published_by_esb = 'N';             
                END IF;
                v_user_node7     := create_node_fnc (v_doc, v_user_node1, 'ItemStructure');
                v_user_node8    := create_node_fnc (v_doc, v_user_node7, 'Structure');
                
                v_item_node     := add_text_node_fnc (v_doc, v_user_node8, 'AssemblyItemNumber', c_struct_rec.Assembly_Item_Number );   
                v_item_node     := add_text_node_fnc (v_doc, v_user_node8, 'StructureName', c_struct_rec.structure_name );
                v_item_node     := add_text_node_fnc (v_doc, v_user_node8, 'StructureType', c_struct_rec.structure_type_name );   
                v_item_node     := add_text_node_fnc (v_doc, v_user_node8, 'OrganizationCode', c_struct_rec.organization_code );
                v_item_node     := add_text_node_fnc (v_doc, v_user_node8, 'OrganizationName', c_struct_rec.organization_name );
                v_item_node     := add_text_node_fnc (v_doc, v_user_node8, 'AssemblyType', c_struct_rec.assembly_type );
                                                                       
                v_user_node9    := create_node_fnc (v_doc, v_user_node8, 'Components');
                
                -- Comp Loop
                FOR c_comp_rec IN c_comp_cur(c_struct_rec.bill_sequence_id)
                LOOP
                    v_user_node10   := create_node_fnc (v_doc, v_user_node9, 'Component');
                    
                    v_item_node     := add_text_node_fnc (v_doc, v_user_node10, 'ComponentItemNumber', c_comp_rec.component_number ); 
                    v_item_node     := add_text_node_fnc (v_doc, v_user_node10, 'ComponentQuantity', c_comp_rec.component_quantity );
                    v_item_node     := add_text_node_fnc (v_doc, v_user_node10, 'OperationSequenceNumber', c_comp_rec.operation_seq_num ); 
                    v_item_node     := add_text_node_fnc (v_doc, v_user_node10, 'ItemSequenceNumber', c_comp_rec.item_num );
                    v_item_node     := add_text_node_fnc (v_doc, v_user_node10, 'PlanningFactor', c_comp_rec.planning_factor ); 
                    --v_item_node     := add_text_node_fnc (v_doc, v_user_node10, 'FromDate', c_comp_rec.effectivity_date );
                    --v_item_node     := add_text_node_fnc (v_doc, v_user_node10, 'ToDate', c_comp_rec.disable_date );
                    
                END LOOP; -- Comp Loop
                
            END LOOP; -- BOM Loop
            
            --GHEPMv2 Changes
            -- Item Categories loop
            FOR c_item_cat_rec IN c_item_cat_cur (c_item_rec.inventory_item_id)
            LOOP
                v_item_cat_node     := create_node_fnc (v_doc, v_user_node1, 'ItemCategoryAssignment');
                v_item_node     := add_text_node_fnc (v_doc, v_item_cat_node, 'CategorySetName', c_item_cat_rec.category_set_name );    
                v_item_node     := add_text_node_fnc (v_doc, v_item_cat_node, 'CategoryName', c_item_cat_rec.category_name );
            END LOOP; 
            
            -- Item Attachment document docs loop
            FOR c_item_attached_rec IN c_item_attached_doc (c_item_rec.inventory_item_id)
            LOOP
                v_item_cat_node     := create_node_fnc (v_doc, v_user_node1, 'ItemAttachments');
                v_item_node     := add_text_node_fnc (v_doc, v_item_cat_node, 'DocumentID', c_item_attached_rec.document_id );    
                v_item_node     := add_text_node_fnc (v_doc, v_item_cat_node, 'DocumentCategory', c_item_attached_rec.user_name );
                v_item_node     := add_text_node_fnc (v_doc, v_item_cat_node, 'DocumentTitle', c_item_attached_rec.title ); 
                v_item_node     := add_text_node_fnc (v_doc, v_item_cat_node, 'DocumentDescription', c_item_attached_rec.description ); 
                v_item_node     := add_text_node_fnc (v_doc, v_item_cat_node, 'DocumentLongText', c_item_attached_rec.long_text ); 
            END LOOP;              
            
        END LOOP; -- Item Loop
            
          
        dbms_lob.createtemporary (v_clob, TRUE);
        xmldom.writetoclob (v_doc, v_clob);
        xmldom.freedocument (v_doc);
      
        -- Adding XML standard header
        v_clob := TO_CLOB('<?xml version="1.0" encoding="UTF-8"?>' || CHR(10) ) || v_clob;
        RETURN v_clob;
      
    EXCEPTION
    WHEN OTHERS THEN
      v_error_msg := SUBSTR (SQLERRM, 1, 1000);
      Log_Msg (V_ERROR_MSG);
      RETURN v_error_msg;      
    END get_xml_message;
    
    
    
   /* ************************************************************************************
   * Function: generate_payload
   * Synopsis: This function will generate the XML payload for a given record of stage table
   * PARAMETERS:
   *        p_record_id
   *
   * Return Values:
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            2-Apr-2016
      |  Akshay Nayak         | 26-Sep-2016   | 1.2      | Integration related changes 
   ************************************************************************************* */         
    --FUNCTION generate_payload (p_record_id  IN NUMBER)
    FUNCTION generate_payload (p_record_id  IN NUMBER
                      ,p_in_entity_name IN VARCHAR2
                      )
        RETURN NUMBER
    AS
        l_rec_count             NUMBER              := 0;
        l_xmlclob               CLOB;
        l_queue_record_id       NUMBER              := 0;
        l_error_message         VARCHAR2(4000);          
        l_xmlfile               SYS.XMLTYPE;
        l_qname                 VARCHAR2(50);
                
    BEGIN
  
        Log_Msg ('Inside generate_payload procedure');        
        Log_Msg ('Calling get_xml_message function');   
        
        --Changes for v1.2
        --l_xmlclob := get_xml_message (p_record_id);
        l_xmlclob := get_xml_message (p_record_id
                         ,p_in_entity_name
                         );

        IF l_xmlclob = empty_clob() 
        THEN
            l_error_message := 'Generated XML message is empty';
            Log_Msg(l_error_message);
        ELSE        
            -- Adding XML standard header
            --l_xmlclob := TO_CLOB('<?xml version="1.0" encoding="UTF-8"?>' || CHR(10) ) || l_xmlclob;
            
            BEGIN 
                SELECT q_name
                  INTO l_qname 
                  FROM xxpso_pdh_outqueue_items_stg 
                 WHERE record_id = p_record_id;
            EXCEPTION
            WHEN OTHERS THEN
                NULL;
            END; 

            Log_Msg ('Inserting the XML Message into XXPSO_PDH_QUEUE_HIST table');  
            BEGIN          
                l_queue_record_id     := xxpso_pdh_queue_hist_S.nextval;
            
                INSERT INTO xxpso_pdh_queue_hist
                    ( 
                     record_id         
                    ,direction         
                    ,q_name            
                    ,msgid                       
                    ,payload           
                    ,created_by        
                    ,creation_date     
                    ,last_updated_by   
                    ,last_update_date       
                    ,status_code             
                    )
                VALUES
                    ( 
                     l_queue_record_id
                    ,'OUT'         
                    ,l_qname
                    ,NULL                      
                    ,l_xmlclob           
                    ,gcn_user_id
                    ,SYSDATE
                    ,gcn_user_id    
                    ,SYSDATE      
                    ,'N'         
                    );                               
                COMMIT;
                
                Log_Msg ('XML Message inserted suceessfully. Queue Record ID : '|| l_queue_record_id);                    
            EXCEPTION
            WHEN OTHERS THEN
                l_error_message := 'Error in inserting into XXPSO_PDH_QUEUE_HIST : ' || SQLERRM;
                Log_Msg(l_error_message);
            END;              
        END IF;

        IF l_error_message IS NULL
        THEN
            UPDATE xxpso_pdh_outqueue_items_stg x
               SET x.status_code        = 'P',
                   x.conc_request_id    = gcn_request_id,
                   x.last_updated_by    = gcn_user_id,
                   x.last_update_date   = SYSDATE,
                   x.last_update_login  = gcn_last_update_login
             WHERE x.status_code        = 'N' 
               AND EXISTS
                   (SELECT 1
                      FROM xxpso_pdh_outqueue_items_stg y
                     WHERE y.inventory_item_id  = x.inventory_item_id
                       AND y.organization_id    = x.organization_id
                       AND y.record_id          = p_record_id
                       AND y.entity_name        = p_in_entity_name    --Changes for v1.2
                    );
            COMMIT;   
            RETURN l_queue_record_id;            
                    
        ELSE
            UPDATE xxpso_pdh_outqueue_items_stg x
               SET x.status_code        = 'F',
                   x.error_message      = l_error_message,
                   x.conc_request_id    = gcn_request_id,
                   x.last_updated_by    = gcn_user_id,
                   x.last_update_date   = SYSDATE,
                   x.last_update_login  = gcn_last_update_login                   
             WHERE x.status_code        = 'N' 
               AND EXISTS
                   (SELECT 1
                      FROM xxpso_pdh_outqueue_items_stg y
                     WHERE y.inventory_item_id  = x.inventory_item_id
                       AND y.organization_id    = x.organization_id
                       AND y.record_id          = p_record_id
                       AND y.entity_name        = p_in_entity_name    --Changes for v1.2
                    );
            COMMIT;   
            
            gn_retcode  := gcn_retcode_warning;            
            
            RETURN -1;                         
        END IF;
        
    EXCEPTION
    WHEN OTHERS
    THEN
        Log_Msg('Error in GENERATE_PAYLOAD function : ' || SQLERRM);
        gn_retcode  := gcn_retcode_warning;        
        RETURN -1;        
    END generate_payload;
    
   /* ************************************************************************************
   * Function: enqueue_payload
   *
   * Synopsis: This function will send the payload to the Outbound Queue based on queue name
   *           and the message passed.
   *
   * PARAMETERS:
   *        p_in_queue_name
   *        p_in_message
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            2-Apr-2016
   ************************************************************************************* */  
    FUNCTION enqueue_payload (p_in_queue_name   IN VARCHAR2
                     ,p_in_message    IN CLOB
                     )
        RETURN VARCHAR2
        IS
        l_message               sys.aq$_jms_text_message;    
        l_queue_options            DBMS_AQ.ENQUEUE_OPTIONS_T;
        l_message_properties    DBMS_AQ.MESSAGE_PROPERTIES_T;    
        l_msgid                 RAW(16);
        l_error_message         VARCHAR2 (4000); 
        BEGIN
        IF l_error_message IS NULL
        THEN                    
            l_message := sys.aq$_jms_text_message.construct;
            l_message.set_text(p_in_message);            
                    
            Log_Msg ('Calling DBMS_AQ.ENQUEUE procedure');     

            -- Enqueuing the message
            DBMS_AQ.ENQUEUE (   queue_name            => p_in_queue_name,    
                                enqueue_options       => l_queue_options,
                                message_properties    => l_message_properties,
                                payload               => l_message,
                                msgid                 => l_msgid
                            );                                 

            Log_Msg ('DBMS_AQ.ENQUEUE procedure completed'); 
            Log_Msg ('MSGID    :' || l_msgid);    
                            
            IF l_msgid IS NULL
            THEN            
                l_error_message := 'Error in DBMS_AQ.ENQUEUE procedure';
                Log_Msg(l_error_message);
            END IF;
       
        END IF;
        RETURN l_error_message;
        EXCEPTION
    WHEN OTHERS THEN
         Log_Msg (l_error_message);
        END enqueue_payload;


   /* ************************************************************************************
   * Function: enqueue_payload
   *
   * Synopsis: This function will send the payload to the Outbound Queue
   *
   * PARAMETERS:
   *        p_queue_record_id
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Narendra Mishra    1.0                                            2-Apr-2016
   ************************************************************************************* */  
    FUNCTION enqueue_payload (p_queue_record_id  IN NUMBER)
        RETURN VARCHAR2
    IS
        l_param_list            wf_parameter_list_t;
        l_param_name            VARCHAR2 (240);
        l_param_value           VARCHAR2 (2000);
        l_event_name            VARCHAR2 (2000);
        l_event_key             VARCHAR2 (2000);
        l_event_data            VARCHAR2 (4000);
        l_xmlclob               CLOB;        
        --v_enqueue_payload       SYSTEM.ecxmsg;
        l_message               sys.aq$_jms_text_message;    
        l_queue_options            DBMS_AQ.ENQUEUE_OPTIONS_T;
        l_message_properties    DBMS_AQ.MESSAGE_PROPERTIES_T;    
        l_msgid                 RAW(16);
        l_ret_sts               VARCHAR2(30);
        l_ret_msg               VARCHAR2(4000);        
        l_error_message         VARCHAR2 (4000); 
        l_qname                 VARCHAR2(50);
        
    BEGIN
            
        Log_Msg ('Inside ENQUEUE_PAYLOAD procedure');
        --Log_Msg ('Queue Name    :' || gc_queue_name);
        Log_Msg ('Queue Record Id     :' || p_queue_record_id);
        
        BEGIN 
          SELECT payload, q_name
            INTO l_xmlclob, l_qname
            FROM xxpso_pdh_queue_hist
           WHERE record_id = p_queue_record_id;
        EXCEPTION
        WHEN OTHERS
        THEN
            l_error_message := 'Queue Record Id - '|| p_queue_record_id || 'Not Found in XXPSO_PDH_QUEUE_HIST';
            Log_Msg (l_error_message);
        END;           

        IF l_error_message IS NULL
        THEN                    
            l_message := sys.aq$_jms_text_message.construct;
            l_message.set_text(l_xmlclob);            
            l_message_properties.correlation   := p_queue_record_id;
                   
            Log_Msg ('Calling DBMS_AQ.ENQUEUE procedure');     

            -- Enqueuing the message
            DBMS_AQ.ENQUEUE (   queue_name            => l_qname,    
                                enqueue_options       => l_queue_options,
                                message_properties    => l_message_properties,
                                payload               => l_message,
                                msgid                 => l_msgid
                            );                                 

            Log_Msg ('DBMS_AQ.ENQUEUE procedure completed'); 
            Log_Msg ('MSGID    :' || l_msgid);    
                            
            IF l_msgid IS NULL
            THEN            
                l_error_message := 'Error in DBMS_AQ.ENQUEUE procedure';
            END IF;
       
        END IF;
                
        IF l_error_message IS NULL
        THEN                
            UPDATE xxpso_pdh_queue_hist
               SET status_code          = 'P',
                   msgid                = l_msgid,
                   corrid               = p_queue_record_id,
                   conc_request_id      = gcn_request_id,
                   last_updated_by      = gcn_user_id,
                   last_update_date     = SYSDATE,
                   last_update_login    = gcn_last_update_login               
             WHERE record_id            = p_queue_record_id;
            COMMIT;
            
            RETURN 'SUCCESS';
                
        ELSE
            UPDATE xxpso_pdh_queue_hist
               SET status_code          = 'F',
                   error_message        = l_error_message,
                   conc_request_id      = gcn_request_id,
                   last_updated_by      = gcn_user_id,
                   last_update_date     = SYSDATE,
                   last_update_login    = gcn_last_update_login                       
             WHERE record_id            = p_queue_record_id;
            COMMIT;   
            
            gn_retcode  := gcn_retcode_warning;    
            RETURN 'ERROR';  
        END IF;        
                      
    EXCEPTION
    WHEN OTHERS
    THEN
        l_error_message := 'Error in ENQUEUE_PAYLOAD ' || SQLERRM;
        Log_Msg (l_error_message);     
        RETURN l_error_message;
        gn_retcode  := gcn_retcode_warning;        
    END enqueue_payload; 
                             
                            
END XXPSO_PRODUCT_UTILITY_PKG;


/
SHOW ERROR

EXEC APPS.XXPSO_INSTALL_PK.VERIFY('XXPSO_PRODUCT_UTILITY_PKG');
EXIT;
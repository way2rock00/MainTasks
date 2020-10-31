CREATE OR REPLACE PACKAGE BODY slc_glext_bal_seg_pkg
AS
--+============================================================|
--+  Program:      slc_glext_bal_seg_bpkg.sql
--+   Author:      Akshay Nayak
--+  Date:         31-Jan-2017
--+ Purpose:       This package will have procedures that will create new value
--+		   in valueset when change over is executed. Also this will enrich DFF value for
--+		   values in valueset.
--+  Change Log 1.0:   23-Jan-2017   Akshay Nayak Created
--+  Change Log 1.1:   27-Feb-2017   Akshay Nayak Updated to fetch Franchisee Information as per latest functional doc.
--+  Change Log 1.2:   1-Mar-2017   Incorporating FUT Comments.
--+  Change Log 1.3:   4-Mar-2017   Changes after getting Business clarification on open items.
--+	 Change Log 1.4:   25-Jul-2017	Changes for Defect 40509. For Corporate Stores we were fetching Supplier Information
--+									based on Segment1. Changing it to vendor_name_alt.
--+	 Change Log 1.5:   02-Nov-2017	CR 2167. Changes to disable flex value if PERMANENT_CLOSED_DATE < sysdate or PERMANENT_CLOSED_DATE
--+									and ACTUAL_OPEN_DATE is null.
--+  Change Log 1.6:   6-Dec-2017   Changes for defect 43994 - ASI II UAT :Segment values are displaying incorrect description and qualifiers
--+  Change Log 1.7:   15-Dec-2017  Changes for defect 43994 - Earlier enabled_flag was set to N when disabling the flex value.
--+									Changing this logic.No need to set enabled_flag to N.
--+  Change Log 1.8:   19-Dec-2017  Changes for defect 44378 - Changing Edition format to CYYMM from YYMM where C represents Century.
--+  Change Log 1.9:   08-Feb-2018  Changes for defect 45072 - Changing BCP Agreement check to 12.
--+  Change Log 1.10:   08-MAY-2018  Changes for defect CR#361563  - Changing from date in the flex values to First day of the Change-over Month.
--+  Change Log 1.11:   06-Feb-2018  Changes for 40% Go Live. Change in derivation logic for Lottery License Held by Franchisee and 
--+									SEI Credit Card Processing
--+============================================================|
gv_log 		VARCHAR2(5) := 'LOG';
gv_out		VARCHAR2(5) := 'OUT';
gv_debug_flag	VARCHAR2(3) ;
gv_yes_code	VARCHAR2(3) := 'YES';
gv_no_code	VARCHAR2(3) := 'NO';
gn_request_id                             NUMBER DEFAULT fnd_global.conc_request_id;
gn_user_id                                NUMBER DEFAULT fnd_global.user_id;
gn_login_id                               NUMBER DEFAULT fnd_global.login_id;

--Variables for Common Error Handling.
gv_batch_key				  VARCHAR2(50) DEFAULT 'FRC-E-082'||'-'||TO_CHAR(SYSDATE,'DDMMYYYY');
gv_business_process_name 		  VARCHAR2(100)  := 'SLC_GLEXT_BAL_SEG_PKG';
gv_cmn_err_rec 				  APPS.SLC_UTIL_JOBS_PKG.G_ERROR_TBL_TYPE;
gv_cmn_err_count			  NUMBER DEFAULT 0;


/* ****************************************************************
	NAME:              slc_write_log_p
	PURPOSE:           This procedure will insert data into either
		    concurrent program log file or in concurrent program output file
		    based on the parameter passed to the input program
	Input Parameters:  p_in_message
			   p_in_log_type
*****************************************************************/
  PROCEDURE slc_write_log_p(p_in_log_type IN VARCHAR2
  		      ,p_in_message IN VARCHAR2)
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

  END slc_write_log_p;


/* ****************************************************************
	NAME:              slc_is_value_set_value_valid_f
	PURPOSE:           This function will take valuesetname and the value.
			   If will validate if the passed value is valid or not.
			   If it is valid it will return Y else it will return N.
	Input Parameters:  p_in_value_set_name
			   p_in_value
*****************************************************************/
  FUNCTION slc_is_value_set_value_valid_f(
  				    p_in_value_set_name IN VARCHAR
  				   ,p_in_value		IN VARCHAR
  				    )
  RETURN VARCHAR2
  IS
  lv_value_valid	VARCHAR2(1) := 'N';

  BEGIN

    BEGIN
	IF p_in_value_set_name = 'SLCGL_LEDGER' THEN
      SELECT 'Y'
        INTO lv_value_valid
        FROM gl_sets_of_books gl
       WHERE gl.short_name = p_in_value;
	ELSIF p_in_value_set_name <> 'SLCGL_LEDGER' THEN
      SELECT 'Y'
        INTO lv_value_valid
        FROM fnd_flex_value_sets ffvs
            ,fnd_flex_values ffv
       WHERE ffvs.flex_value_set_id = ffv.flex_value_set_id
         AND ffv.enabled_flag = 'Y'
         AND ffvs.flex_value_set_name = p_in_value_set_name
         AND ffv.flex_value = p_in_value;
	END IF;
    EXCEPTION
    WHEN OTHERS THEN
      lv_value_valid	:= 'N';
    END;

	slc_write_log_p(gv_log,'p_in_value_set_name: '||p_in_value_set_name||
  		   ' p_in_value:'||p_in_value||
  		   ' Value Exists Flag:'||lv_value_valid
  	    );
  RETURN lv_value_valid;
  END slc_is_value_set_value_valid_f;


	/* ****************************************************************
	NAME:              slc_get_lookup_code_f
	PURPOSE:           This procedure will be used to get lookup code.
	Input Parameters:  p_in_lookup_type		IN VARCHAR2
			   			 p_in_lookup_meaning		IN VARCHAR2
*****************************************************************/
	FUNCTION slc_get_lookup_code_f(p_in_lookup_type		IN VARCHAR2
							   ,p_in_lookup_meaning		IN VARCHAR2)
	RETURN VARCHAR2
	IS
	CURSOR c1
	IS
	SELECT lookup_code
	  FROM fnd_lookup_values
	 WHERE ENABLED_FLAG = 'Y'
       AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
	   AND lookup_type = p_in_lookup_type
	   AND meaning = p_in_lookup_meaning;
	lv_lookup_code		fnd_lookup_values.lookup_code%TYPE;
	BEGIN

	IF p_in_lookup_meaning IS NOT NULL THEN
		OPEN c1;
		FETCH c1 INTO lv_lookup_code;
		CLOSE c1;
	END IF;
	RETURN lv_lookup_code;
	END slc_get_lookup_code_f;

	/* ****************************************************************
	NAME:              slc_get_parent_value_f
	PURPOSE:           This procedure will be used to get parent value for a flex value
	Input Parameters:  p_in_lookup_type		IN VARCHAR2
			   			 p_in_lookup_meaning		IN VARCHAR2
*****************************************************************/
	FUNCTION slc_get_parent_value_f(x_flex_value		IN VARCHAR2 --Flex Value
								   ,X_ATTRIBUTE3		IN VARCHAR2 -- Agreement Type
								   ,x_attribute4		IN VARCHAR2	-- Traditional Banking Flag
								   )
	RETURN VARCHAR2
	IS
	lv_parent_value		VARCHAR2(100);
    lv_slcgl_default_parent VARCHAR2(25) := 'SLCGL_DEFAULT_PARENT';

	CURSOR cur_bcp_value(p_in_agreement_value IN VARCHAR2)
	IS
	SELECT COUNT(1)
	FROM fnd_lookup_values flv
	WHERE flv.lookup_type LIKE 'SLCCN_CRM_RESOURCE'
	AND flv.enabled_flag = 'Y'
	AND meaning         IN ('BCP TRADITIONAL','BCP NON-TRADITIONAL')
	AND (attribute1      = p_in_agreement_value
	OR attribute2        = p_in_agreement_value
	OR attribute3        = p_in_agreement_value
	OR attribute4        = p_in_agreement_value );
	ln_bcp_count		NUMBER DEFAULT 0;
	BEGIN
	slc_write_log_p(gv_log,'Determining parent: X_FLEX_VALUE:'||X_FLEX_VALUE||' Traditional Banking:'||X_ATTRIBUTE4||
							' Agreement Value:'||x_attribute3);
	/* Below the logic of various store letter codes.
	 * If Store Letter Code is between A to G then it is termed as Franchisee.
	 * Franchisee Store are classified as BCP if Agreement Type for that store is present in DFF value for Lookup SLCCN_CRM_RESOURCE.
	 * BCP store are classifed as Traditional BCP and Non Traditional BCP based on Traditional Banking Flag.
	 * If Store Letter Code is between H to P then it is termed as Corporate Stores.
	 * If Store Letter Code is X it means the store is terminated
	 */

   -- A to G indicates that is is Franchised Store
   IF     (SUBSTR (X_FLEX_VALUE, 6, 1) BETWEEN 'A' AND 'G')
	  AND (SUBSTR (X_FLEX_VALUE, 1, 5) BETWEEN '10000'
												 AND '99999'
		  )
   THEN
		--x_attribute3 represent Agreement Type for Store.
		IF x_attribute3 IS NULL THEN
			lv_parent_value := slc_get_lookup_code_f(lv_slcgl_default_parent,'FRANCHISEE');
		ELSE
			OPEN cur_bcp_value(x_attribute3);
			FETCH cur_bcp_value INTO ln_bcp_count;
			CLOSE cur_bcp_value;
			slc_write_log_p(gv_log,'Determining parent: ln_bcp_count:'||ln_bcp_count);

			IF ln_bcp_count > 0 AND X_ATTRIBUTE4 = 'Y' THEN
				lv_parent_value := slc_get_lookup_code_f(lv_slcgl_default_parent,'TRADITIONAL BCP');
			ELSIF ln_bcp_count > 0 AND X_ATTRIBUTE4 = 'N' THEN
				lv_parent_value := slc_get_lookup_code_f(lv_slcgl_default_parent,'NON-TRADITIONAL BCP');
			ELSIF ln_bcp_count = 0 THEN
				lv_parent_value := slc_get_lookup_code_f(lv_slcgl_default_parent,'FRANCHISEE');
			END IF;
		END IF;

		/*
		-- X_ATTRIBUTE18 represents SEI Controls Property and is derived from Hierarchy attribute of Finance Reporting Attribute Group.
		-- If Hierarchy = 3 then X_ATTRIBUTE18 = N else it is Y.
		-- If Hierarchy = 3 then it is a BCP Store.
		-- X_ATTRIBUTE4 represent traditional banking flag.
		IF X_ATTRIBUTE18 = 'N' AND X_ATTRIBUTE4 = 'Y' THEN
			lv_parent_value := slc_get_lookup_code_f(lv_slcgl_default_parent,'TRADITIONAL BCP');
		ELSIF X_ATTRIBUTE18 = 'N' AND X_ATTRIBUTE4 = 'N' THEN
			lv_parent_value := slc_get_lookup_code_f(lv_slcgl_default_parent,'NON-TRADITIONAL BCP');
		ELSIF X_ATTRIBUTE18 = 'N' AND X_ATTRIBUTE4 IS NULL THEN
			lv_parent_value := slc_get_lookup_code_f(lv_slcgl_default_parent,'FRANCHISEE');
		ELSIF X_ATTRIBUTE18 = 'Y' THEN
			lv_parent_value := slc_get_lookup_code_f(lv_slcgl_default_parent,'FRANCHISEE');
		END IF;*/
   END IF;

   -- A to G indicates that is is Corporate Store
   IF     (SUBSTR (X_FLEX_VALUE, 6, 1) BETWEEN 'H' AND 'Z')
	  AND (SUBSTR (X_FLEX_VALUE, 1, 5) BETWEEN '10000'
												 AND '99999'
		  )
   THEN
		lv_parent_value := slc_get_lookup_code_f(lv_slcgl_default_parent,'CORPORATE');
   END IF;

	RETURN lv_parent_value;
	END slc_get_parent_value_f;

/* ****************************************************************
	NAME:              slc_insert_row_p
	PURPOSE:           This procedure will insert record into fnd_flex_values
	****************************************************************/

procedure slc_insert_row_p (
  x_rowid in out nocopy VARCHAR2,
  x_flex_value_id in NUMBER,
  x_attribute_sort_order in NUMBER,
  x_flex_value_set_id in NUMBER,
  x_flex_value in VARCHAR2,
  x_enabled_flag in VARCHAR2,
  x_summary_flag in VARCHAR2,
  x_start_date_active in DATE,
  x_end_date_active in DATE,
  x_parent_flex_value_low in VARCHAR2,
  x_parent_flex_value_high in VARCHAR2,
  x_structured_hierarchy_level in NUMBER,
  x_hierarchy_level in VARCHAR2,
  x_compiled_value_attributes in VARCHAR2,
  x_value_category in VARCHAR2,
  x_attribute1 in VARCHAR2,
  x_attribute2 in VARCHAR2,
  x_attribute3 in VARCHAR2,
  X_ATTRIBUTE4 in VARCHAR2,
  x_attribute5 in VARCHAR2,
  x_attribute6 in VARCHAR2,
  x_attribute7 in VARCHAR2,
  x_attribute8 in VARCHAR2,
  x_attribute9 in VARCHAR2,
  x_attribute10 in VARCHAR2,
  x_attribute11 in VARCHAR2,
  x_attribute12 in VARCHAR2,
  x_attribute13 in VARCHAR2,
  x_attribute14 in VARCHAR2,
  x_attribute15 in VARCHAR2,
  x_attribute16 in VARCHAR2,
  x_attribute17 in VARCHAR2,
  x_attribute18 in VARCHAR2,
  x_attribute19 in VARCHAR2,
  x_attribute20 in VARCHAR2,
  x_attribute21 in VARCHAR2,
  x_attribute22 in VARCHAR2,
  x_attribute23 in VARCHAR2,
  x_attribute24 in VARCHAR2,
  x_attribute25 in VARCHAR2,
  x_attribute26 in VARCHAR2,
  x_attribute27 in VARCHAR2,
  x_attribute28 in VARCHAR2,
  x_attribute29 in VARCHAR2,
  x_attribute30 in VARCHAR2,
  x_attribute31 in VARCHAR2,
  x_attribute32 in VARCHAR2,
  x_attribute33 in VARCHAR2,
  x_attribute34 in VARCHAR2,
  x_attribute35 in VARCHAR2,
  x_attribute36 in VARCHAR2,
  x_attribute37 in VARCHAR2,
  x_attribute38 in VARCHAR2,
  x_attribute39 in VARCHAR2,
  x_attribute40 in VARCHAR2,
  x_attribute41 in VARCHAR2,
  x_attribute42 in VARCHAR2,
  x_attribute43 in VARCHAR2,
  x_attribute44 in VARCHAR2,
  x_attribute45 in VARCHAR2,
  x_attribute46 in VARCHAR2,
  x_attribute47 in VARCHAR2,
  x_attribute48 in VARCHAR2,
  x_attribute49 in VARCHAR2,
  x_attribute50 in VARCHAR2,
  x_flex_value_meaning in VARCHAR2,
  x_description in VARCHAR2,
  x_creation_date in DATE,
  x_created_by in NUMBER,
  x_last_update_date in DATE,
  x_last_updated_by in NUMBER,
  x_last_update_login in NUMBER
) is
  cursor C is select ROWID from FND_FLEX_VALUES
    where FLEX_VALUE_ID = X_FLEX_VALUE_ID
    ;
  lv_parent_value		VARCHAR2(100);
  lv_dupcheck_hier               fnd_flex_values.flex_value%TYPE;

begin
  slc_write_log_p(gv_log,'Inserting value for: '||X_FLEX_VALUE);
  insert into FND_FLEX_VALUES (
    attribute_sort_order,
    flex_value_set_id,
    flex_value_id,
    flex_value,
    enabled_flag,
    summary_flag,
    start_date_active,
    end_date_active,
    parent_flex_value_low,
    parent_flex_value_high,
    structured_hierarchy_level,
    hierarchy_level,
    compiled_value_attributes,
    value_category,
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
    attribute31,
    attribute32,
    attribute33,
    attribute34,
    attribute35,
    attribute36,
    attribute37,
    attribute38,
    attribute39,
    attribute40,
    attribute41,
    attribute42,
    attribute43,
    attribute44,
    attribute45,
    attribute46,
    attribute47,
    attribute48,
    attribute49,
    attribute50,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login
  ) values (
    x_attribute_sort_order,
    x_flex_value_set_id,
    x_flex_value_id,
    x_flex_value,
    x_enabled_flag,
    x_summary_flag,
    x_start_date_active,
    x_end_date_active,
    x_parent_flex_value_low,
    x_parent_flex_value_high,
    x_structured_hierarchy_level,
    x_hierarchy_level,
    x_compiled_value_attributes,
    x_value_category,
    x_attribute1,
    x_attribute2,
    x_attribute3,
    x_attribute4,
    x_attribute5,
    x_attribute6,
    x_attribute7,
    x_attribute8,
    x_attribute9,
    x_attribute10,
    x_attribute11,
    x_attribute12,
    x_attribute13,
    x_attribute14,
    x_attribute15,
    x_attribute16,
    x_attribute17,
    x_attribute18,
    x_attribute19,
    x_attribute20,
    x_attribute21,
    x_attribute22,
    x_attribute23,
    x_attribute24,
    x_attribute25,
    x_attribute26,
    x_attribute27,
    x_attribute28,
    x_attribute29,
    x_attribute30,
    x_attribute31,
    x_attribute32,
    x_attribute33,
    x_attribute34,
    x_attribute35,
    x_attribute36,
    x_attribute37,
    x_attribute38,
    x_attribute39,
    x_attribute40,
    x_attribute41,
    x_attribute42,
    x_attribute43,
    x_attribute44,
    x_attribute45,
    x_attribute46,
    x_attribute47,
    x_attribute48,
    x_attribute49,
    x_attribute50,
    x_creation_date,
    x_created_by,
    x_last_update_date,
    x_last_updated_by,
    x_last_update_login
  );

  insert into FND_FLEX_VALUES_TL (
    flex_value_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    description,
    flex_value_meaning,
    language,
    source_lang
  ) select
    x_flex_value_id,
    x_last_update_date,
    x_last_updated_by,
    x_creation_date,
    x_created_by,
    x_last_update_login,
    x_description,
    x_flex_value_meaning,
    l.language_code,
    userenv('LANG')
  from FND_LANGUAGES L
  where L.INSTALLED_FLAG in ('I', 'B')
  and not exists
    (select NULL
    from FND_FLEX_VALUES_TL T
    where T.FLEX_VALUE_ID = X_FLEX_VALUE_ID
    and T.LANGUAGE = L.LANGUAGE_CODE);

  open c;
  fetch c into X_ROWID;
  if (c%notfound) then
    close c;
    raise no_data_found;
  end if;
  close c;
	lv_parent_value := slc_get_parent_value_f(X_FLEX_VALUE,X_ATTRIBUTE3,X_ATTRIBUTE4);
	slc_write_log_p(gv_log,'Inserting value for parent: '||lv_parent_value);
	IF lv_parent_value IS NOT NULL
	   THEN
		  SELECT MAX (parent_flex_value)
			INTO lv_dupcheck_hier
		   FROM fnd_flex_value_norm_hierarchy
		   WHERE parent_flex_value = lv_parent_value
			 AND child_flex_value_low = X_FLEX_VALUE
			 AND child_flex_value_high = X_FLEX_VALUE
			 AND flex_value_set_id = X_FLEX_VALUE_SET_ID;

		  IF lv_dupcheck_hier IS NOT NULL
		  THEN
			raise too_many_rows;
		  ELSE
			 INSERT INTO fnd_flex_value_norm_hierarchy
						 (flex_value_set_id, parent_flex_value,
						  range_attribute, child_flex_value_low,
						  child_flex_value_high, last_update_date,
						  last_updated_by, creation_date,
						  created_by, last_update_login
						 )
			 VALUES (X_FLEX_VALUE_SET_ID, lv_parent_value,
						  'C', X_FLEX_VALUE,
						  X_FLEX_VALUE, SYSDATE,
						  X_LAST_UPDATED_BY, SYSDATE,
						  X_CREATED_BY, X_LAST_UPDATE_LOGIN
						 );

		  END IF;
	   END IF;
  --Perform commit operation when a record has been successfully inserted in fnd_flex_values.
  COMMIT;
end slc_insert_row_p;

/* ****************************************************************
	NAME:              slc_update_row_p
	PURPOSE:           This procedure will update record into fnd_flex_values
	****************************************************************/
procedure slc_update_row_p (
  x_flex_value_id in NUMBER,
  x_attribute_sort_order in NUMBER,
  x_flex_value_set_id in NUMBER,
  x_flex_value in VARCHAR2,
  x_enabled_flag in VARCHAR2,
  x_summary_flag in VARCHAR2,
  x_start_date_active in DATE,
  x_end_date_active in DATE,
  x_parent_flex_value_low in VARCHAR2,
  x_parent_flex_value_high in VARCHAR2,
  x_structured_hierarchy_level in NUMBER,
  x_hierarchy_level in VARCHAR2,
  x_compiled_value_attributes in VARCHAR2,
  x_value_category in VARCHAR2,
  x_attribute1 in VARCHAR2,
  x_attribute2 in VARCHAR2,
  x_attribute3 in VARCHAR2,
  x_attribute4 in VARCHAR2,
  x_attribute5 in VARCHAR2,
  x_attribute6 in VARCHAR2,
  x_attribute7 in VARCHAR2,
  x_attribute8 in VARCHAR2,
  x_attribute9 in VARCHAR2,
  x_attribute10 in VARCHAR2,
  x_attribute11 in VARCHAR2,
  x_attribute12 in VARCHAR2,
  x_attribute13 in VARCHAR2,
  x_attribute14 in VARCHAR2,
  x_attribute15 in VARCHAR2,
  x_attribute16 in VARCHAR2,
  x_attribute17 in VARCHAR2,
  x_attribute18 in VARCHAR2,
  x_attribute19 in VARCHAR2,
  x_attribute20 in VARCHAR2,
  x_attribute21 in VARCHAR2,
  x_attribute22 in VARCHAR2,
  x_attribute23 in VARCHAR2,
  x_attribute24 in VARCHAR2,
  x_attribute25 in VARCHAR2,
  x_attribute26 in VARCHAR2,
  x_attribute27 in VARCHAR2,
  x_attribute28 in VARCHAR2,
  x_attribute29 in VARCHAR2,
  x_attribute30 in VARCHAR2,
  x_attribute31 in VARCHAR2,
  x_attribute32 in VARCHAR2,
  x_attribute33 in VARCHAR2,
  x_attribute34 in VARCHAR2,
  x_attribute35 in VARCHAR2,
  x_attribute36 in VARCHAR2,
  x_attribute37 in VARCHAR2,
  x_attribute38 in VARCHAR2,
  x_attribute39 in VARCHAR2,
  x_attribute40 in VARCHAR2,
  x_attribute41 in VARCHAR2,
  x_attribute42 in VARCHAR2,
  x_attribute43 in VARCHAR2,
  x_attribute44 in VARCHAR2,
  x_attribute45 in VARCHAR2,
  x_attribute46 in VARCHAR2,
  x_attribute47 in VARCHAR2,
  x_attribute48 in VARCHAR2,
  x_attribute49 in VARCHAR2,
  x_attribute50 in VARCHAR2,
  x_flex_value_meaning in VARCHAR2,
  x_description in VARCHAR2,
  x_update_mode_type	in VARCHAR2,
  x_last_update_date in DATE,
  x_last_updated_by in NUMBER,
  x_last_update_login in NUMBER
) is
 lv_parent_value		VARCHAR2(100);
begin
  slc_write_log_p(gv_log,'Updating value for: '||X_FLEX_VALUE||' X_FLEX_VALUE_ID:'||X_FLEX_VALUE_ID || ' X_UPDATE_MODE_TYPE:'||X_UPDATE_MODE_TYPE);
  slc_write_log_p(gv_log,'Updating value x_start_date_active:'||x_start_date_active);
  update FND_FLEX_VALUES set
    --Changes as per v1.2
	-- As during enrichment we need to update only DFF value
	-- Removed all other attributes apart from DFF values from the update list.
    attribute1 = x_attribute1,
    attribute2 = x_attribute2,
    attribute3 = x_attribute3,
    attribute4 = x_attribute4,
    attribute5 = x_attribute5,
    attribute6 = x_attribute6,
    attribute7 = x_attribute7,
    attribute8 = x_attribute8,
    attribute9 = x_attribute9,
    attribute10 = x_attribute10,
    attribute11 = x_attribute11,
    attribute12 = x_attribute12,
    attribute13 = x_attribute13,
    attribute14 = x_attribute14,
    attribute15 = x_attribute15,
    attribute16 = x_attribute16,
    attribute17 = x_attribute17,
    attribute18 = x_attribute18,
    attribute19 = x_attribute19,
    attribute20 = x_attribute20,
    attribute21 = x_attribute21,
    attribute22 = x_attribute22,
    attribute23 = x_attribute23,
    attribute24 = x_attribute24,
    attribute25 = x_attribute25,
    attribute26 = x_attribute26,
    attribute27 = x_attribute27,
    attribute28 = x_attribute28,
    attribute29 = x_attribute29,
    attribute30 = x_attribute30,
    attribute31 = x_attribute31,
    attribute32 = x_attribute32,
    attribute33 = x_attribute33,
    attribute34 = x_attribute34,
    attribute35 = x_attribute35,
    attribute36 = x_attribute36,
    attribute37 = x_attribute37,
    attribute38 = x_attribute38,
    attribute39 = x_attribute39,
    attribute40 = x_attribute40,
    attribute41 = x_attribute41,
    attribute42 = x_attribute42,
    attribute43 = x_attribute43,
    attribute44 = x_attribute44,
    attribute45 = x_attribute45,
    attribute46 = x_attribute46,
    attribute47 = x_attribute47,
    attribute48 = x_attribute48,
    attribute49 = x_attribute49,
    attribute50 = x_attribute50,
	-- If Store Letter Code is already existing then we will have to enable disabled Flex value.
	enabled_flag = 'Y',
	end_date_active = NULL,
	last_update_date = x_last_update_date,
    last_updated_by = x_last_updated_by,
    last_update_login = x_last_update_login,
	start_date_active = DECODE(X_UPDATE_MODE_TYPE,'OVERRIDE',x_start_date_active,start_date_active)
  where flex_value_id = x_flex_value_id;

    if (sql%notfound) then
    raise no_data_found;
  end if;



  update FND_FLEX_VALUES_TL set
    description = x_description,
    last_update_date = x_last_update_date,
    last_updated_by = x_last_updated_by,
    last_update_login = x_last_update_login,
    source_lang = userenv('lang')
  where FLEX_VALUE_ID = X_FLEX_VALUE_ID
  and userenv('LANG') in (LANGUAGE, SOURCE_LANG);

  /*
    if (sql%notfound) then
    raise no_data_found;
  end if;*/

  IF X_UPDATE_MODE_TYPE = 'OVERRIDE' THEN
    slc_write_log_p(gv_log,'Updating parent value set X_FLEX_VALUE:'||X_FLEX_VALUE||' X_ATTRIBUTE3:'||X_ATTRIBUTE3||
							' X_ATTRIBUTE4:'||X_ATTRIBUTE4);
	lv_parent_value := slc_get_parent_value_f(X_FLEX_VALUE,X_ATTRIBUTE3,X_ATTRIBUTE4);
	slc_write_log_p(gv_log,'Updating value for parent: '||lv_parent_value);
	IF lv_parent_value IS NOT NULL THEN
	  update fnd_flex_value_norm_hierarchy set
		parent_flex_value = lv_parent_value,
		LAST_UPDATE_DATE = X_LAST_UPDATE_DATE,
		LAST_UPDATED_BY = X_LAST_UPDATED_BY,
		LAST_UPDATE_LOGIN = X_LAST_UPDATE_LOGIN
	  where flex_value_set_id = X_FLEX_VALUE_SET_ID
	    AND child_flex_value_low = X_FLEX_VALUE
		AND child_flex_value_high = X_FLEX_VALUE;

	    if (sql%notfound) then
		--If there is no parent value associated then insert parent record.
			 INSERT INTO fnd_flex_value_norm_hierarchy
						 (flex_value_set_id, parent_flex_value,
						  range_attribute, child_flex_value_low,
						  child_flex_value_high, last_update_date,
						  last_updated_by, creation_date,
						  created_by, last_update_login
						 )
			 VALUES (X_FLEX_VALUE_SET_ID, lv_parent_value,
						  'C', X_FLEX_VALUE,
						  X_FLEX_VALUE, SYSDATE,
						  X_LAST_UPDATED_BY, SYSDATE,
						  x_last_updated_by, X_LAST_UPDATE_LOGIN
						 );
			--raise no_data_found;
		end if;

	END IF;
  END IF;
  --Perform commit operation when a record has been successfully inserted in fnd_flex_values.
  COMMIT;
end SLC_UPDATE_ROW_P;

/* ****************************************************************
	NAME:              slc_load_row_p
	PURPOSE:           This procedure will load data into fnd_flex_values
	****************************************************************/
  PROCEDURE slc_load_row_p
  (x_flex_value_set_name          IN VARCHAR2,
   x_parent_flex_value_low        IN VARCHAR2,
   x_flex_value                   IN VARCHAR2,
   x_who                          IN fnd_flex_loader_apis.who_type,
   x_enabled_flag                 IN VARCHAR2,
   x_summary_flag                 IN VARCHAR2,
   x_start_date_active            IN DATE,
   x_end_date_active              IN DATE,
   x_parent_flex_value_high       IN VARCHAR2,
   x_structured_hierarchy_level   IN NUMBER,
   x_hierarchy_level              IN VARCHAR2,
   x_compiled_value_attributes    IN VARCHAR2,
   x_value_category               IN VARCHAR2,
   x_attribute1                   IN VARCHAR2,
   x_attribute2                   IN VARCHAR2,
   x_attribute3                   IN VARCHAR2,
   x_attribute4                   IN VARCHAR2,
   x_attribute5                   IN VARCHAR2,
   x_attribute6                   IN VARCHAR2,
   x_attribute7                   IN VARCHAR2,
   x_attribute8                   IN VARCHAR2,
   x_attribute9                   IN VARCHAR2,
   x_attribute10                  IN VARCHAR2,
   x_attribute11                  IN VARCHAR2,
   x_attribute12                  IN VARCHAR2,
   x_attribute13                  IN VARCHAR2,
   x_attribute14                  IN VARCHAR2,
   x_attribute15                  IN VARCHAR2,
   x_attribute16                  IN VARCHAR2,
   x_attribute17                  IN VARCHAR2,
   x_attribute18                  IN VARCHAR2,
   x_attribute19                  IN VARCHAR2,
   x_attribute20                  IN VARCHAR2,
   x_attribute21                  IN VARCHAR2,
   x_attribute22                  IN VARCHAR2,
   x_attribute23                  IN VARCHAR2,
   x_attribute24                  IN VARCHAR2,
   x_attribute25                  IN VARCHAR2,
   x_attribute26                  IN VARCHAR2,
   x_attribute27                  IN VARCHAR2,
   x_attribute28                  IN VARCHAR2,
   x_attribute29                  IN VARCHAR2,
   x_attribute30                  IN VARCHAR2,
   x_attribute31                  IN VARCHAR2,
   x_attribute32                  IN VARCHAR2,
   x_attribute33                  IN VARCHAR2,
   x_attribute34                  IN VARCHAR2,
   x_attribute35                  IN VARCHAR2,
   x_attribute36                  IN VARCHAR2,
   x_attribute37                  IN VARCHAR2,
   x_attribute38                  IN VARCHAR2,
   x_attribute39                  IN VARCHAR2,
   x_attribute40                  IN VARCHAR2,
   x_attribute41                  IN VARCHAR2,
   x_attribute42                  IN VARCHAR2,
   x_attribute43                  IN VARCHAR2,
   x_attribute44                  IN VARCHAR2,
   x_attribute45                  IN VARCHAR2,
   x_attribute46                  IN VARCHAR2,
   x_attribute47                  IN VARCHAR2,
   x_attribute48                  IN VARCHAR2,
   x_attribute49                  IN VARCHAR2,
   x_attribute50                  IN VARCHAR2,
   x_attribute_sort_order         IN VARCHAR2,
   x_flex_value_meaning           IN VARCHAR2,
   x_description                  IN VARCHAR2,
   X_MODE 						  IN VARCHAR2,
   X_UPDATE_MODE_TYPE			  IN VARCHAR2,
   X_FLEX_VALUE_ID 				  IN NUMBER
   )
  IS
     l_flex_value_set_id NUMBER := NULL;
     l_flex_value_id     NUMBER;
     l_rowid             VARCHAR2(64);
BEGIN
   slc_write_log_p(gv_log,'In  slc_load_row_p: x_flex_value_set_name:'||x_flex_value_set_name||
					' x_flex_value:'||x_flex_value||' X_MODE:'||X_MODE||' X_FLEX_VALUE_ID:'||X_FLEX_VALUE_ID);
   SELECT flex_value_set_id
     INTO l_flex_value_set_id
     FROM fnd_flex_value_sets
     WHERE flex_value_set_name = x_flex_value_set_name;

	 slc_write_log_p(gv_log,'l_flex_value_set_id: '||l_flex_value_set_id);



	IF X_MODE = 'UPDATE' THEN
	 SELECT flex_value_id
	   INTO l_flex_value_id
	   FROM fnd_flex_values
	   WHERE flex_value = x_flex_value
	     AND flex_value_set_id = l_flex_value_set_id;
	slc_write_log_p(gv_log,'x_flex_value: '||x_flex_value);

      SLC_UPDATE_ROW_P
	(x_flex_value_id                => l_flex_value_id,
         x_attribute_sort_ordeR         => x_attribute_sort_order,
	 x_flex_value_set_id            => l_flex_value_set_id,
	 x_flex_value                   => x_flex_value,
	 x_enabled_flag                 => x_enabled_flag,
	 x_summary_flag                 => x_summary_flag,
	 x_start_date_active            => x_start_date_active,
	 x_end_date_active              => x_end_date_active,
	 x_parent_flex_value_low        => x_parent_flex_value_low,
	 x_parent_flex_value_high       => x_parent_flex_value_high,
	 x_structured_hierarchy_level   => x_structured_hierarchy_level,
	 x_hierarchy_level              => x_hierarchy_level,
	 x_compiled_value_attributes    => x_compiled_value_attributes,
	 x_value_category               => x_value_category,
	 x_attribute1                   => x_attribute1,
	 x_attribute2                   => x_attribute2,
	 x_attribute3                   => x_attribute3,
	 x_attribute4                   => x_attribute4,
	 x_attribute5                   => x_attribute5,
	 x_attribute6                   => x_attribute6,
	 x_attribute7                   => x_attribute7,
	 x_attribute8                   => x_attribute8,
	 x_attribute9                   => x_attribute9,
	 x_attribute10                  => x_attribute10,
	 x_attribute11                  => x_attribute11,
	 x_attribute12                  => x_attribute12,
	 x_attribute13                  => x_attribute13,
	 x_attribute14                  => x_attribute14,
	 x_attribute15                  => x_attribute15,
	 x_attribute16                  => x_attribute16,
	 x_attribute17                  => x_attribute17,
	 x_attribute18                  => x_attribute18,
	 x_attribute19                  => x_attribute19,
	 x_attribute20                  => x_attribute20,
	 x_attribute21                  => x_attribute21,
	 x_attribute22                  => x_attribute22,
	 x_attribute23                  => x_attribute23,
	 x_attribute24                  => x_attribute24,
	 x_attribute25                  => x_attribute25,
	 x_attribute26                  => x_attribute26,
	 x_attribute27                  => x_attribute27,
	 x_attribute28                  => x_attribute28,
	 x_attribute29                  => x_attribute29,
	 x_attribute30                  => x_attribute30,
	 x_attribute31                  => x_attribute31,
	 x_attribute32                  => x_attribute32,
	 x_attribute33                  => x_attribute33,
	 x_attribute34                  => x_attribute34,
	 x_attribute35                  => x_attribute35,
	 x_attribute36                  => x_attribute36,
	 x_attribute37                  => x_attribute37,
	 x_attribute38                  => x_attribute38,
	 x_attribute39                  => x_attribute39,
	 x_attribute40                  => x_attribute40,
	 x_attribute41                  => x_attribute41,
	 x_attribute42                  => x_attribute42,
	 x_attribute43                  => x_attribute43,
	 x_attribute44                  => x_attribute44,
	 x_attribute45                  => x_attribute45,
	 x_attribute46                  => x_attribute46,
	 x_attribute47                  => x_attribute47,
	 x_attribute48                  => x_attribute48,
	 x_attribute49                  => x_attribute49,
	 x_attribute50                  => x_attribute50,
	 x_flex_value_meaning           => x_flex_value_meaning,
	 x_description                  => x_description,
	 x_update_mode_type				=> x_update_mode_type,
	 x_last_update_date             => x_who.last_update_date,
	 x_last_updated_by              => x_who.last_updated_by,
	 x_last_update_login            => x_who.last_update_login);

	ELSIF X_MODE = 'CREATE' THEN
	 SLC_INSERT_ROW_P
	   (x_rowid                        => l_rowid,
	    x_flex_value_id                => X_FLEX_VALUE_ID,
            x_attribute_sort_order         => x_attribute_sort_order,
	    x_flex_value_set_id            => l_flex_value_set_id,
	    x_flex_value                   => x_flex_value,
	    x_enabled_flag                 => x_enabled_flag,
	    x_summary_flag                 => x_summary_flag,
	    x_start_date_active            => x_start_date_active,
	    x_end_date_active              => x_end_date_active,
	    x_parent_flex_value_low        => x_parent_flex_value_low,
	    x_parent_flex_value_high       => x_parent_flex_value_high,
	    x_structured_hierarchy_level   => x_structured_hierarchy_level,
	    x_hierarchy_level              => x_hierarchy_level,
	    x_compiled_value_attributes    => x_compiled_value_attributes,
	    x_value_category               => x_value_category,
	    x_attribute1                   => x_attribute1,
	    x_attribute2                   => x_attribute2,
	    x_attribute3                   => x_attribute3,
	    x_attribute4                   => x_attribute4,
	    x_attribute5                   => x_attribute5,
	    x_attribute6                   => x_attribute6,
	    x_attribute7                   => x_attribute7,
	    x_attribute8                   => x_attribute8,
	    x_attribute9                   => x_attribute9,
	    x_attribute10                  => x_attribute10,
	    x_attribute11                  => x_attribute11,
	    x_attribute12                  => x_attribute12,
	    x_attribute13                  => x_attribute13,
	    x_attribute14                  => x_attribute14,
	    x_attribute15                  => x_attribute15,
	    x_attribute16                  => x_attribute16,
	    x_attribute17                  => x_attribute17,
	    x_attribute18                  => x_attribute18,
	    x_attribute19                  => x_attribute19,
	    x_attribute20                  => x_attribute20,
	    x_attribute21                  => x_attribute21,
	    x_attribute22                  => x_attribute22,
	    x_attribute23                  => x_attribute23,
	    x_attribute24                  => x_attribute24,
	    x_attribute25                  => x_attribute25,
	    x_attribute26                  => x_attribute26,
	    x_attribute27                  => x_attribute27,
	    x_attribute28                  => x_attribute28,
	    x_attribute29                  => x_attribute29,
	    x_attribute30                  => x_attribute30,
	    x_attribute31                  => x_attribute31,
	    x_attribute32                  => x_attribute32,
	    x_attribute33                  => x_attribute33,
	    x_attribute34                  => x_attribute34,
	    x_attribute35                  => x_attribute35,
	    x_attribute36                  => x_attribute36,
	    x_attribute37                  => x_attribute37,
	    x_attribute38                  => x_attribute38,
	    x_attribute39                  => x_attribute39,
	    x_attribute40                  => x_attribute40,
	    x_attribute41                  => x_attribute41,
	    x_attribute42                  => x_attribute42,
	    x_attribute43                  => x_attribute43,
	    x_attribute44                  => x_attribute44,
	    x_attribute45                  => x_attribute45,
	    x_attribute46                  => x_attribute46,
	    x_attribute47                  => x_attribute47,
	    x_attribute48                  => x_attribute48,
	    x_attribute49                  => x_attribute49,
	    x_attribute50                  => x_attribute50,
  	    x_flex_value_meaning           => x_flex_value_meaning,
	    x_description                  => x_description,
	    x_creation_date                => x_who.creation_date,
  	    x_created_by                   => x_who.created_by,
	    x_last_update_date             => x_who.last_update_date,
	    x_last_updated_by              => x_who.last_updated_by,
	    x_last_update_login            => x_who.last_update_login);
	END IF;

END slc_load_row_p;

/* ****************************************************************
	NAME:              slc_populate_err_object_p
	PURPOSE:           This procedure will keep on inserting error records
	 		   in the error table.
	Input Parameters:  p_in_batch_key
			   p_in_business_entity
			   p_in_process_id1
			   p_in_process_id2
			   p_in_error_code
			   p_in_error_txt
			   p_in_request_id
			   p_in_attribute1
			   p_in_attribute2
			   p_in_attribute3
			   p_in_attribute4
			   p_in_attribute5
*****************************************************************/
  PROCEDURE slc_populate_err_object_p(p_in_batch_key		IN VARCHAR2
  			       ,p_in_business_entity 	IN VARCHAR2
  			       ,p_in_process_id1	IN VARCHAR2 DEFAULT NULL
  			       ,p_in_process_id2	IN VARCHAR2 DEFAULT NULL
				   ,p_in_process_id3	IN VARCHAR2 DEFAULT NULL
				   ,p_in_process_id4	IN VARCHAR2 DEFAULT NULL
				   ,p_in_process_id5	IN VARCHAR2 DEFAULT NULL
				   ,p_in_business_process_step IN VARCHAR2 DEFAULT NULL
  			       ,p_in_error_code		IN VARCHAR2 DEFAULT NULL
  			       ,p_in_error_txt		IN VARCHAR2
  			       ,p_in_request_id		IN NUMBER
  			       ,p_in_attribute1		IN VARCHAR2 DEFAULT NULL
  			       ,p_in_attribute2		IN VARCHAR2 DEFAULT NULL
  			       ,p_in_attribute3		IN VARCHAR2 DEFAULT NULL
  			       ,p_in_attribute4		IN VARCHAR2 DEFAULT NULL
  			       ,p_in_attribute5		IN VARCHAR2 DEFAULT NULL
  			       )
  IS
  BEGIN

      gv_cmn_err_count := gv_cmn_err_count + 1;
      gv_cmn_err_rec(gv_cmn_err_count).seq := SLC_UTIL_BATCH_KEY_S.NEXTVAL;
      gv_cmn_err_rec(gv_cmn_err_count).business_process_entity   := p_in_business_entity;
      gv_cmn_err_rec(gv_cmn_err_count).business_process_id1      := p_in_process_id1;
      gv_cmn_err_rec(gv_cmn_err_count).business_process_id2      := p_in_process_id2;
	  gv_cmn_err_rec(gv_cmn_err_count).business_process_id3      := p_in_process_id3;
	  gv_cmn_err_rec(gv_cmn_err_count).business_process_id4      := p_in_process_id4;
	  gv_cmn_err_rec(gv_cmn_err_count).business_process_id5      := p_in_process_id5;
	  gv_cmn_err_rec(gv_cmn_err_count).business_process_step      := p_in_business_process_step;
      gv_cmn_err_rec(gv_cmn_err_count).ERROR_CODE                := p_in_error_code;
      gv_cmn_err_rec(gv_cmn_err_count).ERROR_TEXT                := p_in_error_txt;
      gv_cmn_err_rec(gv_cmn_err_count).request_id                := p_in_request_id;
      gv_cmn_err_rec(gv_cmn_err_count).attribute1                := p_in_attribute1;
      gv_cmn_err_rec(gv_cmn_err_count).attribute2                := p_in_attribute2;
      gv_cmn_err_rec(gv_cmn_err_count).attribute3                := p_in_attribute3;
      gv_cmn_err_rec(gv_cmn_err_count).attribute4                := p_in_attribute4;
      gv_cmn_err_rec(gv_cmn_err_count).attribute5                := p_in_attribute5;
  END slc_populate_err_object_p;


/* ****************************************************************
	NAME:              slc_load_flex_value_p
	PURPOSE:           This procedure will Store Letter Code and Site Id as parameter.
			   For the Site Id information passed this will fetch UDA information
			   from Oracle Site Hub and it will call FND_FLEX_VALUES_PKG.slc_load_row_p
			   to update information in fnd_flex_values
	Input Parameters:  p_in_store_letter_code
			   p_in_site_id
			   p_in_start_date_effective
	Output Parameters: p_out_err_flag
			   p_out_err_msg

--+ 27-Feb-2017  1.1 Akshay Nayak 	Updated to fetch Franchisee Information as per latest functional doc.
*****************************************************************/

  PROCEDURE slc_load_flex_value_p(p_in_store_letter_code 	IN VARCHAR2
   			   ,p_in_site_id	   	IN NUMBER
			   ,p_in_mode			IN VARCHAR2
			   ,p_in_update_mode	IN VARCHAR2
			   ,p_in_flex_id		IN NUMBER
   			   ,p_out_err_flag	   	OUT VARCHAR2
               ,p_out_err_msg	   	OUT VARCHAR2
  			   )
  IS
	--Changes for v1.3 Begin
	--ln_ssn_id			fnd_flex_values.attribute1%TYPE;
	lv_supplier_number		ap_suppliers.segment1%TYPE;
	--Changes for v1.3 End
	lv_franchise_name		fnd_flex_values.attribute1%TYPE;
	ln_franchise_number		ap_suppliers.segment1%TYPE;
	lv_franchise_draw_type	fnd_flex_values.attribute1%TYPE;
 	lv_store_effective_date	fnd_flex_values.attribute1%TYPE ;
	ld_flex_start_date		fnd_flex_values.start_date_active%TYPE;
  	lv_store_termination_date	fnd_flex_values.attribute1%TYPE  DEFAULT NULL;
  	lv_agreement_edition		fnd_flex_values.attribute1%TYPE;
  lv_multiple_indicator		fnd_flex_values.attribute1%TYPE;--Need to fetch information from Supplier Hub
 	lv_agreement_type		fnd_flex_values.attribute1%TYPE;
  lv_traditional_bank		fnd_flex_values.attribute1%TYPE;--Need to write logic for this. As it not clear.
	lv_gas_location		fnd_flex_values.attribute1%TYPE;
  	lv_gas_integration		fnd_flex_values.attribute1%TYPE;
  	lv_car_wash_fac		fnd_flex_values.attribute1%TYPE; --Calculation based.
  	lv_send_lottery		fnd_flex_values.attribute1%TYPE;
 	lv_sei_hvac_main		fnd_flex_values.attribute1%TYPE;
  	lv_sei_light_main		fnd_flex_values.attribute1%TYPE;
  	lv_sei_payroll		fnd_flex_values.attribute1%TYPE;
	lv_sei_control_property	fnd_flex_values.attribute1%TYPE;  --Calculation based.
  	lv_sei_credit_card		fnd_flex_values.attribute1%TYPE;
  	lv_sei_gas_proceed		fnd_flex_values.attribute1%TYPE;
	lv_set_of_books_id		fnd_flex_values.attribute1%TYPE;
	lv_company_code			fnd_flex_values.attribute1%TYPE;
	lv_compiled_value_attribute		fnd_flex_values.COMPILED_VALUE_ATTRIBUTES%TYPE;
	lv_store_effective_date_desc VARCHAR2(40); --added for v1.6

	lx_who APPS.FND_FLEX_LOADER_APIS.WHO_TYPE;

	--Changes for v1.1 Begin
	ln_extension_id			rrs_sites_ext_b.extension_id%TYPE;
	--Changes for v1.1 End

  lv_business_process_id1		VARCHAR2(25) DEFAULT 'SLC_LOAD_FLEX_VALUE_P';
  lv_err_flag				VARCHAR2(1) DEFAULT 'N';
  lv_err_msg				VARCHAR2(4000);
  lv_attr_group_type			VARCHAR2(30) DEFAULT 'RRS_SITEMGMT_GROUP';
  lv_pos_sup_group_type			 VARCHAR2(30) DEFAULT 'POS_SUPP_PROFMGMT_GROUP';
  lv_application_name 			VARCHAR2(5)  DEFAULT	'RRS';
  lv_application_name1 			VARCHAR2(5)  DEFAULT	'POS';
  lv_contractual_attributes		VARCHAR2(50) DEFAULT 'SLC_SM_CONTRACTUAL_ATTRIBUTES';
  lv_franchisee_details			VARCHAR2(50) DEFAULT 'SLC_SM_FRANCHISEE_DETAILS';
  lv_pos_franchisee_details   VARCHAR2(50) DEFAULT 'SLC_ISP_FRANCHISEE_DETAILS';
  lv_agreement				VARCHAR2(50) DEFAULT  'SLC_SM_AGREEMENT';
  lv_fuel_operation			VARCHAR2(50) DEFAULT  'SLC_SM_FUEL_OPERATION';
  lv_fuel_store				VARCHAR2(50) DEFAULT  'SLC_SM_FUEL_STORE';
  lv_ss_car_wash			VARCHAR2(50) DEFAULT  'SLC_SM_SS_CAR_WASH';
  lv_ss_lottery				VARCHAR2(50) DEFAULT  'SLC_SM_SS_LOTTERY';
  lv_store_operator_attr		VARCHAR2(50) DEFAULT  'SLC_SM_STORE_OPERATOR_ATTR';
  lv_fin_reporting			VARCHAR2(50) DEFAULT  'SLC_SM_FIN_REP';
  lv_store_operator			VARCHAR2(50) DEFAULT  'SLC_SM_STORE_OPERATOR';
  lv_store_media			VARCHAR2(50) DEFAULT  'SLC_SM_STORE_MEDIA';
  lv_yes_no_vs				VARCHAR2(50) DEFAULT  'SLCGL_BCP_YES_NO';
  lv_agreement_edition_vs		VARCHAR2(50) DEFAULT  'SLCPRC_EDITION';
  lv_gas_integration_vs			VARCHAR2(50) DEFAULT  'SLCGL_BCP_GAS_INTEGRATION';
  lv_agreement_type_vs			VARCHAR2(50) DEFAULT  'SLCGL_BCP_AGREEMENT_TYPE';
  lv_yes_no_both_vs			VARCHAR2(50) DEFAULT  'SLCGL_BCP_YES_NO_BOTH';
  lv_set_of_books_vs			VARCHAR2(50) DEFAULT  'SLCGL_LEDGER';

  BEGIN

   lx_who.created_by   := gn_user_id;
   lx_who.creation_date   := sysdate;
   lx_who.last_updated_by  := gn_user_id;
   lx_who.last_update_date  := sysdate;
   lx_who.last_update_login := gn_user_id;
   lv_err_msg				:= 'Store Letter Code:'||p_in_store_letter_code||' Site Id:'||p_in_site_id;
   slc_write_log_p(gv_log,'In slc_load_flex_value_p p_in_store_letter_code:'||p_in_store_letter_code||' p_in_site_id:'||p_in_site_id||
					' p_in_mode:'||p_in_mode||' p_in_update_mode:'||p_in_update_mode||' p_in_flex_id:'||p_in_flex_id);


   --If store letter code is X i.e. if a Store is closed then we need all DFF values to be populated as NULL.
	-- Since store letter code is null there is no meaning to DFF values and hence keeping it as NULL.
	--Changes for v1.4
	-- As part of defect we are reverting code changes. For Special Store Letter Code we are recalculating everything now.
   /*
   IF  substr(p_in_store_letter_code,-1,1) IN ( 'X' ,'T' ,'Z','S') THEN
		slc_write_log_p(gv_log,'In slc_load_flex_value_p INACTIVE -- X Store Letter Code');
		lv_supplier_number		:= NULL;
		lv_franchise_name		:= NULL;
		lv_franchise_draw_type	:= NULL;
		lv_store_effective_date	:= NULL;
		ld_flex_start_date		:= NULL;
		lv_store_termination_date	:= NULL;
		lv_agreement_edition 		:= NULL;
		lv_agreement_type			:= NULL;
		lv_traditional_bank			:= NULL;
		lv_gas_location				:= NULL;
		lv_multiple_indicator		:= NULL;
		lv_gas_integration			:= NULL;
		lv_car_wash_fac				:= NULL;
		lv_send_lottery				:= NULL;
		lv_sei_hvac_main 			:= NULL;
		lv_sei_light_main 			:= NULL;
		lv_sei_payroll 				:= NULL;
		lv_sei_gas_proceed 	    	:= NULL;
		lv_sei_control_property		:= NULL;
		lv_sei_credit_card			:= NULL;
		lv_set_of_books_id			:= NULL;
   ELSE
   */
   -- Get franchise details
   -- Fetch details for SSN Tax Id/ Supplier Name from Supplier Hub.
   --Changes for v1.1 Begin
   -- We can have 3 types of Franchise Information for any Store.
   -- 1. Store can have one primary Franchisee. In this case Incorporation Flag would be N and Ownership Status of the person would be PRIMARY.
   -- 2. Store can have 2 franchisee's. In this case for both franchisee Incorporation Flag would be N. One of them would be PRIMARY and other one would
   -- 	 secondary
   -- 3. Store can have franchisee with Incorporation flag as Y.
   -- If in a case if Incorpration flag is Y for any store we need to pick franchisee Details for Franchisee for which Incorporation Flag is Y , else
   -- we will pick franchisee information from Primary Franchisee.
	BEGIN

	--For Corporate Stores dummy supplier will be created. That will be Franchisee name.
	IF SUBSTR(p_in_store_letter_code,6,1) BETWEEN 'H' AND 'Z' THEN
		SELECT pv.segment1 , pv.vendor_name_alt
		  INTO lv_supplier_number , lv_franchise_name
		  FROM ap_suppliers pv
		 WHERE pv.vendor_name_alt = '7-Eleven Inc.';
		 --Changes Log 1.4
		 --pv.segment1 = '10001001';

	ELSIF SUBSTR(p_in_store_letter_code,6,1) BETWEEN 'A' AND 'G' THEN
		--This select statement will select Franchisee Information for which Incorporation Flag is Y
		SELECT  pv.segment1
				--Changes for v1.3 Earlier it was tax_payer_id. As part of this change log it has to be Supplier Number.
				/*NVL(	DECODE(upper(pv.vendor_type_lookup_code), 'EMPLOYEE', papf.national_identifier,
							DECODE(pv.organization_type_lookup_code, 'INDIVIDUAL',pv.individual_1099,
																	'FOREIGN INDIVIDUAL',pv.individual_1099,
																	hp.jgzz_fiscal_code
									)
							), pvud.n_ext_attr1) hz_taxpayer_id ,*/
			   ,pv.vendor_name_alt
			   ,rrs1.c_ext_attr5 --n_ext_attr1		Earlier column name was n_ext_attr1. Now Franchisee Number is configured in c_ext_attr5
		INTO lv_supplier_number
			,lv_franchise_name
			,ln_franchise_number
		FROM (SELECT rrs.extension_id
			FROM rrs_sites_ext_b rrs
				,ego_attr_groups_v eagv
				,fnd_application fnda
			WHERE rrs.attr_group_id         = eagv.attr_group_id
			AND eagv.attr_group_type        = lv_attr_group_type
			AND eagv.attr_group_name        = lv_franchisee_details
			AND eagv.application_id         = fnda.application_id
			AND fnda.application_short_name = lv_application_name
			AND rrs.c_ext_attr2             = 'Yes'	--Incorporation
			--Changes for v1.4
			--Commented IS NULL check condition. Now we are checking sysdate in between Effective Start Date and Effective End Date
			AND SYSDATE BETWEEN TRUNC(NVL(rrs.D_EXT_ATTR1,SYSDATE)) AND TRUNC(NVL(rrs.D_EXT_ATTR2,SYSDATE+1))
			--AND rrs.D_EXT_ATTR2             IS NULL --Effective End Date
			AND rrs.site_id = p_in_site_id
			UNION
			SELECT rrs.extension_id
			FROM rrs_sites_ext_b rrs
				,ego_attr_groups_v eagv
				,fnd_application fnda
			WHERE rrs.attr_group_id         = eagv.attr_group_id
			AND eagv.attr_group_type        = lv_attr_group_type
			AND eagv.attr_group_name        = lv_franchisee_details
			AND eagv.application_id         = fnda.application_id
			AND fnda.application_short_name = lv_application_name
			AND rrs.c_ext_attr2             = 'No'	--Incorporation
			--Changes for v1.4
			--Commented IS NULL check condition. Now we are checking sysdate in between Effective Start Date and Effective End Date
			AND SYSDATE BETWEEN TRUNC(NVL(rrs.D_EXT_ATTR1,SYSDATE)) AND TRUNC(NVL(rrs.D_EXT_ATTR2,SYSDATE+1))
			--AND rrs.D_EXT_ATTR2             IS NULL --Effective End Date
			AND rrs.c_ext_attr3             = 'Primary'
			AND rrs.site_id = p_in_site_id
			AND NOT EXISTS (SELECT 1
			FROM rrs_sites_ext_b rrs
				,ego_attr_groups_v eagv
				,fnd_application fnda
			WHERE rrs.attr_group_id         = eagv.attr_group_id
			AND eagv.attr_group_type        = lv_attr_group_type
			AND eagv.attr_group_name        = lv_franchisee_details
			AND eagv.application_id         = fnda.application_id
			AND fnda.application_short_name = lv_application_name
			AND rrs.c_ext_attr2             = 'Yes'	--Incorporation
			--Changes for v1.4
			--Commented IS NULL check condition. Now we are checking sysdate in between Effective Start Date and Effective End Date
			AND SYSDATE BETWEEN TRUNC(NVL(rrs.D_EXT_ATTR1,SYSDATE)) AND TRUNC(NVL(rrs.D_EXT_ATTR2,SYSDATE+1))
			--AND rrs.D_EXT_ATTR2             IS NULL --Effective End Date
			AND rrs.site_id = p_in_site_id
			)) fran_table ,
			rrs_sites_ext_b rrs1 ,
			ap_suppliers pv
			/*per_all_people_f papf ,
			hz_parties hp
			, ego_attr_groups_v eagv1
				, fnd_application fnda1
				, POS_SUPP_PROF_EXT_VL pvud*/
		WHERE rrs1.extension_id         = fran_table.extension_id
		--Earlier column name was n_ext_attr1. Now Franchisee Number is configured in c_ext_attr5
		AND pv.segment1                 = TO_CHAR(rrs1.c_ext_attr5)
		--AND pv.party_id                 = hp.party_id
		--AND pv.employee_id              = papf.person_id (+)
		AND rrs1.site_id                 = p_in_site_id;
		--AND eagv1.attr_group_type        = lv_pos_sup_group_type
		--AND eagv1.attr_group_name        = lv_pos_franchisee_details
		--AND eagv1.application_id         = fnda1.application_id
		--AND fnda1.application_short_name = lv_application_name1
		--AND eagv1.attr_group_id = pvud.attr_group_id(+)
		--AND hp.party_id = pvud.party_id(+);

	END IF;

	EXCEPTION
	WHEN NO_DATA_FOUND THEN
     	 ln_franchise_number	:= NULL;
     	 lv_supplier_number	:= NULL;
		 lv_franchise_name := NULL;
     	 lv_err_flag := 'Y';
     	 lv_err_msg  := lv_err_msg||'~ No Data found for Supplier for Store Letter Code:'||p_in_store_letter_code;

   	WHEN OTHERS THEN
     	 ln_franchise_number	:= NULL;
		lv_supplier_number	:= NULL;
         lv_franchise_name	:= NULL;
     	 lv_err_flag := 'Y';
     	 lv_err_msg  := lv_err_msg||'~ Error while fetching information from Attribute Group SLC_SM_CONTRACTUAL_ATTRIBUTES for Store Letter Code:'||p_in_store_letter_code||
										' Error Message.'||SQLERRM;

    END;
    slc_write_log_p(gv_log,'slc_load_flex_value_p: Error Flag:'||lv_err_flag||' ln_franchise_number:'||ln_franchise_number||
    		     ' lv_supplier_number:'||lv_supplier_number||' lv_franchise_name:'||lv_franchise_name);


	--Get Franchisee Draw Type.
	IF SUBSTR(p_in_store_letter_code,6,1) BETWEEN 'A' AND 'G' THEN --For Franchisee Stores
	lv_franchise_draw_type := 'M';
	ELSIF SUBSTR(p_in_store_letter_code,6,1) BETWEEN 'H' AND 'Z' THEN --For Corporate Stores
	lv_franchise_draw_type := NULL;
	END IF;

	--Get Effective Start Date. Effective Start Date is Actual Change Over Date from Active Operator group.
    BEGIN
      SELECT to_char(D_EXT_ATTR1 , 'YYYYMMDD'), (Last_Day(ADD_MONTHS(d_ext_attr1,-1))+1) --> Changed to pick the first date of the Month instead of actual change-over date. As per CR#361563
			 --d_ext_attr1  -- Commented for the effective start_date change as per CR # 361563
        INTO lv_store_effective_date,
			 ld_flex_start_date 
	FROM rrs_sites_ext_b rrs
	  ,ego_attr_groups_v eagv
	  ,fnd_application fnda
	WHERE rrs.attr_group_id         = eagv.attr_group_id
	AND eagv.attr_group_type        = lv_attr_group_type
	AND eagv.attr_group_name        = lv_store_operator
	AND eagv.application_id         = fnda.application_id
	AND fnda.application_short_name = lv_application_name
	AND rrs.site_id = p_in_site_id;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
     lv_store_effective_date	:= NULL;
	 ld_flex_start_date			:= NULL;
    WHEN OTHERS THEN
     lv_store_effective_date	:= NULL;
	 ld_flex_start_date	 		:= NULL;
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~ Error while fetching Effective Start Date for Store Letter Code '||p_in_store_letter_code||'. Error Message.'||SQLERRM;
    END;
	slc_write_log_p(gv_log,'slc_load_flex_value_p Error Flag:'||lv_err_flag||' lv_store_effective_date: '||lv_store_effective_date);

	--Get Agreement Edition and Agreement Type value from SLC_SM_AGREEMENT Attribute group
	--As this Attribute group is multi row we need to fetch information for row for which Effective End Date is null.
    BEGIN
	/*
	IF SUBSTR(p_in_store_letter_code,6,1) BETWEEN 'H' AND 'Z' THEN
		lv_agreement_edition := '09/2006';--Value for Agreement Edition.
		lv_agreement_type	 := '02'; -- Value for Agreement Type = GGPS from Valueset SLCGL_BCP_AGREEMENT_TYPE.
	ELSIF SUBSTR(p_in_store_letter_code,6,1) BETWEEN 'A' AND 'G' THEN
	*/
	--Changes for v1.8 Earlier edition was in format YYMM. As per defect expected format is CYYMM where C= Century
	-- If YY = 19 then C = 0. If YY = 20 then C = 1
	--substring is needed because if we to to_char(date,'YY') then we last 2 digits i.e if year is 2006 then we get 06.
	--Thus we perform YYYY and then take first 2 characters using substring.
	  SELECT DECODE( substr(to_char(to_date(c_ext_attr4,'MM/YYYY'),'YYYY'),1,2 ),19,0,20,1) ||
		to_char(to_date(c_ext_attr4,'MM/YYYY'),'YYMM') , c_ext_attr1
		INTO lv_agreement_edition , lv_agreement_type
		FROM rrs_sites_ext_b rrs
		  ,ego_attr_groups_v eagv
		  ,fnd_application fnda
		WHERE rrs.attr_group_id         = eagv.attr_group_id
		AND eagv.attr_group_type        = lv_attr_group_type
		AND eagv.attr_group_name        = lv_agreement
		AND eagv.application_id         = fnda.application_id
		AND fnda.application_short_name = lv_application_name
		--Changes for v1.4
		--Commented IS NULL check condition. Now we are checking sysdate in between Effective Start Date and Effective End Date
		AND SYSDATE BETWEEN TRUNC(NVL(rrs.D_EXT_ATTR3,SYSDATE)) AND TRUNC(NVL(rrs.D_EXT_ATTR4,SYSDATE+1))
		--AND rrs.D_EXT_ATTR4		IS NULL
		AND rrs.site_id = p_in_site_id;
	--END IF;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
     lv_agreement_edition	:= NULL;
     lv_agreement_type		:= NULL;

    WHEN OTHERS THEN
     lv_agreement_edition	:= NULL;
     lv_agreement_type		:= NULL;
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~Error while fetching information from Attribute Group SLC_SM_AGREEMENT for Store Letter Code '||p_in_store_letter_code||'. Error Message.'||SQLERRM;

    END;
    slc_write_log_p(gv_log,'slc_load_flex_value_p: Error Flag:'||lv_err_flag||' lv_agreement_edition:'||
							lv_agreement_edition||' lv_agreement_type:'||lv_agreement_type);

	IF lv_agreement_type IS NOT NULL THEN
	BEGIN
		SELECT ffvt.description
		  INTO lv_agreement_type
		  FROM fnd_flex_value_sets ffvs
			  ,fnd_flex_values ffv
			  ,fnd_flex_values_tl ffvt
		 WHERE ffvs.flex_value_set_name = 'SLCSM_AGREEMENT_TYPE'
			AND ffvs.flex_value_set_id = ffv.flex_value_set_id
			AND ffv.enabled_flag = 'Y'
			AND ffv.flex_value = lv_agreement_type
			AND ffv.flex_value_id = ffvt.flex_value_id;

	EXCEPTION
    WHEN NO_DATA_FOUND THEN
     lv_agreement_type	:= NULL;
	WHEN OTHERS THEN
	 lv_agreement_type := NULL;
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~ Unexpected error while validating Agreement Type:'||lv_agreement_type|| ' Error Message:'||SQLERRM;
	END;

	END IF;

	slc_write_log_p(gv_log,'slc_load_flex_value_p: Error Flag:'||lv_err_flag||' lv_agreement_type:'||lv_agreement_type);
    IF lv_agreement_type IS NOT NULL AND slc_is_value_set_value_valid_f(lv_agreement_type_vs ,lv_agreement_type ) = 'N' THEN
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~ Invalid value for Agreement Type:'||lv_agreement_type;
    END IF;

	IF SUBSTR(p_in_store_letter_code,6,1) BETWEEN 'A' AND 'G' AND lv_agreement_type IS NULL THEN
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~ Agreement Type cannot be null for Franchisee Stores';
	END IF;
	-- Get Traditional Bank information.
    BEGIN
      SELECT DECODE(c_ext_attr8, 'No' ,'N' ,'Yes' , 'Y' , NULL)
        INTO lv_traditional_bank
	FROM rrs_sites_ext_b rrs
	  ,ego_attr_groups_v eagv
	  ,fnd_application fnda
	WHERE rrs.attr_group_id         = eagv.attr_group_id
	AND eagv.attr_group_type        = lv_attr_group_type
	AND eagv.attr_group_name        = lv_contractual_attributes
	AND eagv.application_id         = fnda.application_id
	AND fnda.application_short_name = lv_application_name
	AND rrs.site_id = p_in_site_id;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
     lv_traditional_bank	:= NULL;
    WHEN OTHERS THEN
     lv_traditional_bank	:= NULL;
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~ Error while fetching information from Attribute Group SLC_SM_CONTRACTUAL_ATTRIBUTES,Attribute Traditional Banking. Error Message.'||SQLERRM;

    END;
    slc_write_log_p(gv_log,'slc_load_flex_value_p: Error Flag:'||lv_err_flag||' lv_traditional_bank:'||lv_traditional_bank);
    IF lv_traditional_bank IS NOT NULL AND slc_is_value_set_value_valid_f(lv_yes_no_vs ,lv_traditional_bank ) = 'N' THEN
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~ Invalid value for Traditional Bank:'||lv_traditional_bank;

    END IF;


	-- Get Gas Location from SLC_SM_FUEL_OPERATION Attribute Group.
    BEGIN
      SELECT DECODE(c_ext_attr1, 'No' ,'N' ,'Yes' , 'Y' , NULL)
        INTO lv_gas_location
	FROM rrs_sites_ext_b rrs
	  ,ego_attr_groups_v eagv
	  ,fnd_application fnda
	WHERE rrs.attr_group_id         = eagv.attr_group_id
	AND eagv.attr_group_type        = lv_attr_group_type
	AND eagv.attr_group_name        = lv_fuel_operation
	AND eagv.application_id         = fnda.application_id
	AND fnda.application_short_name = lv_application_name
	AND rrs.site_id = p_in_site_id;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
     lv_gas_location	:= NULL;
    WHEN OTHERS THEN
     lv_gas_location	:= NULL;
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~ Error while fetching information from Attribute Group SLC_SM_FUEL_OPERATION. Error Message.'||SQLERRM;

    END;
    slc_write_log_p(gv_log,'slc_load_flex_value_p: Error Flag:'||lv_err_flag||' lv_gas_location:'||lv_gas_location);

	-- Set multiple indicator
		BEGIN
		IF SUBSTR(p_in_store_letter_code,6,1) BETWEEN 'A' AND 'G' THEN
			SELECT DECODE(count(*), 0, 'N',1,'N', 'Y')
			INTO lv_multiple_indicator
			FROM
			  (SELECT rrs.extension_id,
				rrs.*
			  FROM rrs_sites_ext_b rrs ,
				ego_attr_groups_v eagv ,
				fnd_application fnda
			  WHERE rrs.attr_group_id         = eagv.attr_group_id
			  AND eagv.attr_group_type        = lv_attr_group_type
			  AND eagv.attr_group_name        = lv_franchisee_details
			  AND eagv.application_id         = fnda.application_id
			  AND fnda.application_short_name = lv_application_name
			  AND rrs.c_ext_attr2             = 'Yes' --Incorporation
				--Changes for v1.4
				--Commented IS NULL check condition. Now we are checking sysdate in between Effective Start Date and Effective End Date
			  AND SYSDATE BETWEEN TRUNC(NVL(rrs.D_EXT_ATTR1,SYSDATE)) AND TRUNC(NVL(rrs.D_EXT_ATTR2,SYSDATE+1))
				--AND rrs.D_EXT_ATTR2             IS NULL --Effective End Date
			  AND rrs.c_ext_attr5 = ln_franchise_number
			  UNION
			  SELECT rrs.extension_id,
				rrs.*
			  FROM rrs_sites_ext_b rrs ,
				ego_attr_groups_v eagv ,
				fnd_application fnda
			  WHERE rrs.attr_group_id         = eagv.attr_group_id
			  AND eagv.attr_group_type        = lv_attr_group_type
			  AND eagv.attr_group_name        = lv_franchisee_details
			  AND eagv.application_id         = fnda.application_id
			  AND fnda.application_short_name = lv_application_name
			  AND rrs.c_ext_attr2             = 'No' --Incorporation
				--Changes for v1.4
				--Commented IS NULL check condition. Now we are checking sysdate in between Effective Start Date and Effective End Date
			  AND SYSDATE BETWEEN TRUNC(NVL(rrs.D_EXT_ATTR1,SYSDATE)) AND TRUNC(NVL(rrs.D_EXT_ATTR2,SYSDATE+1))
				--AND rrs.D_EXT_ATTR2             IS NULL --Effective End Date
			  AND rrs.c_ext_attr3 = 'Primary'
			  AND rrs.c_ext_attr5 = ln_franchise_number
			  AND NOT EXISTS
				(SELECT 1
				FROM rrs_sites_ext_b rrs ,
				  ego_attr_groups_v eagv ,
				  fnd_application fnda
				WHERE rrs.attr_group_id         = eagv.attr_group_id
				AND eagv.attr_group_type        = lv_attr_group_type
				AND eagv.attr_group_name        = lv_franchisee_details
				AND eagv.application_id         = fnda.application_id
				AND fnda.application_short_name = lv_application_name
				AND rrs.c_ext_attr2             = 'Yes' --Incorporation
				  --Changes for v1.4
				  --Commented IS NULL check condition. Now we are checking sysdate in between Effective Start Date and Effective End Date
				AND SYSDATE BETWEEN TRUNC(NVL(rrs.D_EXT_ATTR1,SYSDATE)) AND TRUNC(NVL(rrs.D_EXT_ATTR2,SYSDATE+1))
				  --AND rrs.D_EXT_ATTR2             IS NULL --Effective End Date
				AND rrs.c_ext_attr5 = ln_franchise_number
				)
			  ) ;
		 ELSE
			lv_multiple_indicator := NULL;
		 END IF;

    EXCEPTION
    WHEN NO_DATA_FOUND THEN
     lv_multiple_indicator	:= NULL;
    WHEN OTHERS THEN
     lv_multiple_indicator	:= NULL;
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~ Error while fetching information from Attribute Group SLC_SM_CONTRACTUAL_ATTRIBUTES,Attribute Multiple Indicator. Error Message.'||SQLERRM;

    END;
    slc_write_log_p(gv_log,'slc_load_flex_value_p: Error Flag:'||lv_err_flag||' lv_multiple_indicator:'||lv_multiple_indicator);


	-- Get Gas Integration value from SLC_SM_FUEL_STORE Attribute Group.
    BEGIN
      SELECT CASE
			WHEN c_ext_attr3 = 'Fully Integrated Gasoline' THEN 'Y'
			WHEN c_ext_attr3 = '2B Partially Integrated Gasoline' THEN 'P'
			WHEN c_ext_attr3 = '2B Hawaii Gasoline' THEN 'N'
			WHEN c_ext_attr3 = 'Third Party Gasoline' THEN 'N'
				ELSE NULL
			END CASE
        INTO lv_gas_integration
	FROM rrs_sites_ext_b rrs
	  ,ego_attr_groups_v eagv
	  ,fnd_application fnda
	WHERE rrs.attr_group_id         = eagv.attr_group_id
	AND eagv.attr_group_type        = lv_attr_group_type
	AND eagv.attr_group_name        = lv_fuel_store
	AND eagv.application_id         = fnda.application_id
	AND fnda.application_short_name = lv_application_name
	AND rrs.site_id = p_in_site_id;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
     lv_gas_integration	:= NULL;

    WHEN OTHERS THEN
     lv_gas_integration	:= NULL;
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~Error while fetching information from Attribute Group SLC_SM_FUEL_STORE. Error Message.'||SQLERRM;

    END;
    slc_write_log_p(gv_log,'slc_load_flex_value_p: Error Flag:'||lv_err_flag||' lv_gas_integration:'||lv_gas_integration);



    -- Get Car Wash Facility value from SLC_SM_SS_CAR_WASH Attribute Group.
    BEGIN
      SELECT DECODE(c_ext_attr1 , 'None' , 'N' , NULL , NULL, 'Y' )
        INTO lv_car_wash_fac
		FROM rrs_sites_ext_b rrs
		  ,ego_attr_groups_v eagv
		  ,fnd_application fnda
		WHERE rrs.attr_group_id         = eagv.attr_group_id
		AND eagv.attr_group_type        = lv_attr_group_type
		AND eagv.attr_group_name        = lv_ss_car_wash
		AND eagv.application_id         = fnda.application_id
		AND fnda.application_short_name = lv_application_name
		AND rrs.site_id = p_in_site_id;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
     lv_car_wash_fac	:= NULL;

    WHEN OTHERS THEN
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~Error while fetching information from Attribute Group SLC_SM_SS_CAR_WASH. Error Message.'||SQLERRM;

    END;
    slc_write_log_p(gv_log,'slc_load_flex_value_p: Error Flag:'||lv_err_flag||' lv_car_wash_fac:'||lv_car_wash_fac);

	-- Get Lottery Information value from SLC_SM_SS_LOTTERY Attribute Group.
	--Changes for v1.11
	--Earlier FRAN_HELD_LICENSE was fetched. Now we are fetching LOTTERY_LICENSE.
    BEGIN
      SELECT --DECODE(c_ext_attr2 , 'No' ,'N' ,'Yes' , 'Y' , NULL) 
	  DECODE(c_ext_attr13 , 'No' ,'N' ,'Yes' , 'Y' , 'TBD','N', NULL) --Changes for v1.11
        INTO lv_send_lottery
	FROM rrs_sites_ext_b rrs
	  ,ego_attr_groups_v eagv
	  ,fnd_application fnda
	WHERE rrs.attr_group_id         = eagv.attr_group_id
	AND eagv.attr_group_type        = lv_attr_group_type
	AND eagv.attr_group_name        = lv_contractual_attributes--lv_ss_lottery  --Changes for v1.11
	AND eagv.application_id         = fnda.application_id
	AND fnda.application_short_name = lv_application_name
	AND rrs.site_id = p_in_site_id;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
     lv_send_lottery	:= NULL;

    WHEN OTHERS THEN
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~Error while fetching information for Attribute Group SLC_SM_CONTRACTUAL_ATTRIBUTES,Attribute Lottery License. Error Message.'||SQLERRM; 

    END;

    slc_write_log_p(gv_log,'slc_load_flex_value_p: Error Flag:'||lv_err_flag||' Lottery License:'||lv_send_lottery);


	-- Get SEI HVAC Maintenance , SEI Lighting Maintenance , SEI Processing Payroll , Gas Proceeds By FZ.
    BEGIN
      SELECT DECODE(c_ext_attr6, 'No' ,'N' ,'Yes' , 'Y' , NULL ),
			 DECODE(c_ext_attr7, 'No' ,'N' ,'Yes' , 'Y' , NULL ),
			 DECODE(c_ext_attr9, 'No' ,'N' ,'Yes' , 'Y' , NULL ),
			 DECODE(c_ext_attr10, 'No' ,'N' ,'Yes' , 'Y' , NULL)
        INTO lv_sei_hvac_main , lv_sei_light_main , lv_sei_payroll , lv_sei_gas_proceed
	FROM rrs_sites_ext_b rrs
	  ,ego_attr_groups_v eagv
	  ,fnd_application fnda
	WHERE rrs.attr_group_id         = eagv.attr_group_id
	AND eagv.attr_group_type        = lv_attr_group_type
	AND eagv.attr_group_name        = lv_contractual_attributes
	AND eagv.application_id         = fnda.application_id
	AND fnda.application_short_name = lv_application_name
	AND rrs.site_id = p_in_site_id;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
     lv_sei_hvac_main	:= NULL;
     lv_sei_light_main	:= NULL;
     lv_sei_payroll	:= NULL;
     lv_sei_gas_proceed	:= NULL;

    WHEN OTHERS THEN
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~Error while fetching information from Attribute Group SLC_SM_STORE_OPERATOR_ATTR. Error Message.'||SQLERRM;

    END;
    slc_write_log_p(gv_log,'slc_load_flex_value_p: Error Flag:'||lv_err_flag||' lv_sei_hvac_main:'||lv_sei_hvac_main||' lv_sei_light_main:'||lv_sei_light_main);
    slc_write_log_p(gv_log,'slc_load_flex_value_p: Error Flag:'||lv_err_flag||' lv_sei_payroll:'||lv_sei_payroll||' lv_sei_gas_proceed:'||lv_sei_gas_proceed);

    IF lv_sei_hvac_main IS NOT NULL AND slc_is_value_set_value_valid_f(lv_yes_no_vs ,lv_sei_hvac_main ) = 'N' THEN
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~ Invalid value for SEI HVAC Maintenance:'||lv_sei_hvac_main;

    END IF;

    IF lv_sei_light_main IS NOT NULL AND slc_is_value_set_value_valid_f(lv_yes_no_vs ,lv_sei_light_main ) = 'N' THEN
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~ Invalid value for SEI Lighting Maintenance:'||lv_sei_light_main;

    END IF;

    IF lv_sei_payroll IS NOT NULL AND slc_is_value_set_value_valid_f(lv_yes_no_vs ,lv_sei_payroll ) = 'N' THEN
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~ Invalid value for SEI Processing Payroll:'||lv_sei_payroll;

    END IF;

    IF lv_sei_gas_proceed IS NOT NULL AND slc_is_value_set_value_valid_f(lv_yes_no_vs ,lv_sei_gas_proceed ) = 'N' THEN
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~ Invalid value for Gas Proceeds Retained by FZ:'||lv_sei_gas_proceed;

    END IF;

    -- Get SEI Controls Property value from SLC_SM_STORE_OPERATIONS Attribute Group.
	-- If Agreement type fetched above is Traditional Agreement then it is BCP Store otherwise it is Non BCP.
    BEGIN
		IF lv_agreement_type IS NULL THEN
			lv_sei_control_property := NULL;
		--Changes for v1.9 Start
		--ELSIF lv_agreement_type = 'BCP Agreement' THEN
		ELSIF lv_agreement_type = '12' THEN
		--Changes for v1.9 End
			lv_sei_control_property := 'N';
		ELSE
			lv_sei_control_property := 'Y';
		END IF;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
     lv_sei_control_property	:= NULL;

    WHEN OTHERS THEN
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~Error while fetching information from Attribute Group SLC_SM_STORE_OPERATIONS. Error Message.'||SQLERRM;

    END;
    slc_write_log_p(gv_log,'slc_load_flex_value_p: Error Flag:'||lv_err_flag||' lv_sei_control_property:'||lv_sei_control_property);


    -- Get SEI Credit Card Processor value from SLC_SM_STORE_MEDIA Attribute Group.
	--Changes for v1.11
	--Earlier BANK_CARD_PROCESSOR was fetched. Now we are fetching SEI_CREDIT_CARD_PROCESSING.
    BEGIN
      SELECT --DECODE(c_ext_attr1 ,'0000-None' , 'N' , NULL,NULL,'Y')
	  DECODE(c_ext_attr14 ,'No' , 'N','Yes','Y','B','B',NULL) --Changes for v1.11
        INTO lv_sei_credit_card
	FROM rrs_sites_ext_b rrs
	  ,ego_attr_groups_v eagv
	  ,fnd_application fnda
	WHERE rrs.attr_group_id         = eagv.attr_group_id
	AND eagv.attr_group_type        = lv_attr_group_type
	AND eagv.attr_group_name        = lv_contractual_attributes --lv_store_media	--Changes for v1.11
	AND eagv.application_id         = fnda.application_id
	AND fnda.application_short_name = lv_application_name
	AND rrs.site_id = p_in_site_id;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
     lv_sei_credit_card	:= NULL;

    WHEN OTHERS THEN
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~Error while fetching information from Attribute Group SLC_SM_CONTRACTUAL_ATTRIBUTES,Attribute SEI_CREDIT_CARD_PROCESSING. Error Message.'||SQLERRM;  

    END;

	slc_write_log_p(gv_log,'slc_load_flex_value_p: Error Flag:'||lv_err_flag||' lv_sei_credit_card:'||lv_sei_credit_card);
    IF lv_sei_credit_card IS NOT NULL AND slc_is_value_set_value_valid_f(lv_yes_no_both_vs ,lv_sei_credit_card ) = 'N' THEN
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~ Invalid value for SEI Credit Card Processor:'||lv_sei_credit_card;
    END IF;

	--Get Set of Books information.
	slc_write_log_p(gv_log,'slc_load_flex_value_p: Store Number:'||LPAD(SUBSTR(p_in_store_letter_code,1,LENGTH(p_in_store_letter_code)-1),7,'0'));
	BEGIN
		SELECT attribute10
		 INTO lv_company_code
		FROM fnd_flex_value_sets ffvs
			,fnd_flex_values ffv
		WHERE ffvs.flex_value_set_name = 'SLCGL_LOCATION'
		  AND ffvs.flex_value_set_id = ffv.flex_value_set_id
		  AND ffv.flex_value = LPAD(SUBSTR(p_in_store_letter_code,1,LENGTH(p_in_store_letter_code)-1),7,'0');
	EXCEPTION
	WHEN NO_DATA_FOUND THEN
	 lv_company_code := NULL;
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~Store Number not present in Locations DFF.';

    WHEN OTHERS THEN
	 lv_company_code := NULL;
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~Error while fetching Store Number from Locations DFF. Error Message.'||SQLERRM;
    END;
	slc_write_log_p(gv_log,'slc_load_flex_value_p: Error Flag:'||lv_err_flag||' lv_company_code:'||lv_company_code);

	IF lv_company_code IS NOT NULL THEN
		BEGIN
			SELECT meaning
			 INTO lv_set_of_books_id
			FROM fnd_lookup_values flv
			WHERE flv.lookup_type =  'SLCGL_MAP_COMPANY_TO_COUNTRY'
			AND flv.lookup_code = lv_company_code
			AND flv.enabled_flag = 'Y'
			AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE));
		EXCEPTION
		WHEN NO_DATA_FOUND THEN
		 lv_set_of_books_id := NULL;
		 lv_err_flag := 'Y';
		 lv_err_msg  := lv_err_msg||'~Set of Books information not present.';

		WHEN OTHERS THEN
		 lv_set_of_books_id := NULL;
		 lv_err_flag := 'Y';
		 lv_err_msg  := lv_err_msg||'~Error while fetching Set of Books information. Error Message.'||SQLERRM;
		END;
	END IF;
	slc_write_log_p(gv_log,'slc_load_flex_value_p: Error Flag:'||lv_err_flag||' lv_set_of_books_id:'||lv_set_of_books_id);
    IF lv_set_of_books_id IS NOT NULL AND slc_is_value_set_value_valid_f(lv_set_of_books_vs ,lv_set_of_books_id ) = 'N' THEN
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~ Invalid value for Set of Books Id:'||lv_set_of_books_id;
    END IF;

	--Changes for v1.4
	--END IF;

	--Changes for v1.3 Allow Budgeting is set to N.
	-- chr(10) is needed because when we save it from frontend even though value in compiled_value_attributes is seen as NY
	-- there is hidden character. If we dont add this hidden character value is not shown when flex value is opened from frontend.
	lv_compiled_value_attribute := 'N'||chr(10)||'Y';
    slc_write_log_p(gv_log,'slc_load_flex_value_p: Final Flag : '||lv_err_flag);

	--Changes for v1.6 Begin
	BEGIN
	lv_store_effective_date_desc := NVL(to_char(to_date(lv_store_effective_date,'YYYYMMDD'),'MMDDYY'),lv_store_effective_date);
	EXCEPTION
	WHEN OTHERS THEN
     lv_err_flag := 'Y';
     lv_err_msg  := lv_err_msg||'~ Exception while converting date.lv_store_effective_date :'||lv_store_effective_date;
	END;
	--Changes for v1.6 End


    IF lv_err_flag = 'N' THEN
      BEGIN
	  slc_load_row_p(
	    X_FLEX_VALUE_SET_NAME => 'SLCGL_BALANCING_SEGMENT',
	    X_PARENT_FLEX_VALUE_LOW => NULL,
		--Remove leading zero's from Store Letter Code before we create value.
		--Changes for v1.2
	    X_FLEX_VALUE => p_in_store_letter_code,
	    X_WHO => lx_who,
	    X_ENABLED_FLAG => 'Y',
	    X_SUMMARY_FLAG => 'N',
	    X_START_DATE_ACTIVE => ld_flex_start_date,
	    X_END_DATE_ACTIVE => NULL,
	    X_PARENT_FLEX_VALUE_HIGH => NULL,
	    X_STRUCTURED_HIERARCHY_LEVEL => NULL,
	    X_HIERARCHY_LEVEL => NULL,
	    X_COMPILED_VALUE_ATTRIBUTES => lv_compiled_value_attribute,
	    X_VALUE_CATEGORY => 'SLCGL_BALANCING_SEGMENT',
	    X_ATTRIBUTE1 => lv_supplier_number,--SSN/Tax ID
	    X_ATTRIBUTE2 => lv_franchise_name,--Franchise/Owner Name
	    X_ATTRIBUTE3 => lv_agreement_type,--Agreement Type
	    X_ATTRIBUTE4 => lv_traditional_bank,--Traditional Banking
	    X_ATTRIBUTE5 => lv_gas_location,--Gas Location
	    X_ATTRIBUTE6 => lv_gas_integration,--Gas Integration
	    X_ATTRIBUTE7 => lv_car_wash_fac,--Car Wash Facility
	    X_ATTRIBUTE8 => lv_franchise_draw_type,--Franchisee Draw Type
	    X_ATTRIBUTE9 => NULL,
	    X_ATTRIBUTE10 => lv_send_lottery,--Send Lottery $ to Franchisee
	    X_ATTRIBUTE11 => lv_sei_hvac_main,--SEI HVAC Maintenance
	    X_ATTRIBUTE12 => lv_sei_light_main,--SEI Lighting Maintenance
	    X_ATTRIBUTE13 => lv_store_effective_date,--Store Effective Date
	    X_ATTRIBUTE14 => lv_store_termination_date,--Store Termination Date
	    X_ATTRIBUTE15 => lv_agreement_edition,--Agreement Edition
	    X_ATTRIBUTE16 => lv_multiple_indicator,--Multiple Indicator
	    X_ATTRIBUTE17 => lv_sei_payroll,--SEI Processing Payroll
	    X_ATTRIBUTE18 => lv_sei_control_property,--SEI Controls Property
	    X_ATTRIBUTE19 => lv_sei_credit_card,--SEI Credit Card Processor
	    X_ATTRIBUTE20 => lv_sei_gas_proceed,--Gas Proceeds Retained by FZ
	    X_ATTRIBUTE21 => lv_set_of_books_id,--Set of Books ID.
	    X_ATTRIBUTE22 => NULL,
	    X_ATTRIBUTE23 => NULL,
	    X_ATTRIBUTE24 => NULL,
	    X_ATTRIBUTE25 => NULL,
	    X_ATTRIBUTE26 => NULL,
	    X_ATTRIBUTE27 => NULL,
	    X_ATTRIBUTE28 => NULL,
	    X_ATTRIBUTE29 => NULL,
	    X_ATTRIBUTE30 => NULL,
	    X_ATTRIBUTE31 => NULL,
	    X_ATTRIBUTE32 => NULL,
	    X_ATTRIBUTE33 => NULL,
	    X_ATTRIBUTE34 => NULL,
	    X_ATTRIBUTE35 => NULL,
	    X_ATTRIBUTE36 => NULL,
	    X_ATTRIBUTE37 => NULL,
	    X_ATTRIBUTE38 => NULL,
	    X_ATTRIBUTE39 => NULL,
	    X_ATTRIBUTE40 => NULL,
	    X_ATTRIBUTE41 => NULL,
	    X_ATTRIBUTE42 => NULL,
	    X_ATTRIBUTE43 => NULL,
	    X_ATTRIBUTE44 => NULL,
	    X_ATTRIBUTE45 => NULL,
	    X_ATTRIBUTE46 => NULL,
	    X_ATTRIBUTE47 => NULL,
	    X_ATTRIBUTE48 => NULL,
	    X_ATTRIBUTE49 => NULL,
	    X_ATTRIBUTE50 => NULL,
	    X_ATTRIBUTE_SORT_ORDER => NULL,
		--Remove leading zero's from Store Letter Code before we create value.
		--Changes for v1.2
	    X_FLEX_VALUE_MEANING =>  p_in_store_letter_code,
	    X_DESCRIPTION =>  lv_store_effective_date_desc||' '||lv_franchise_name ,   --changes for v1.6
		--Changes for v1.2 End
		X_MODE => p_in_mode,
		X_UPDATE_MODE_TYPE => p_in_update_mode,
		X_FLEX_VALUE_ID => p_in_flex_id
	  );
      EXCEPTION
      WHEN OTHERS THEN
     	lv_err_flag := 'Y';
     	lv_err_msg  := lv_err_msg||'~Error while loading flex value. Error Message.'||SQLERRM;
      END;
    END IF;

    p_out_err_flag := lv_err_flag;
    p_out_err_msg  := lv_err_msg;

  END slc_load_flex_value_p;

/* ****************************************************************
	NAME:              slc_enrich_bal_seg_p
	PURPOSE:           This procedure will be called from concurrent program. Based on the
			   store letter code passed as parameter to program it will decide whether to enrich all the values
			   or to create new value in valueset
	Input Parameters:  p_in_store_letter_code
			   p_debug_flag
*****************************************************************/
  PROCEDURE slc_enrich_bal_seg_p (
		p_errbuf                 OUT      VARCHAR2
		,p_retcode                OUT      NUMBER
		,p_in_store_letter_code    IN     VARCHAR2
		,p_debug_flag              IN     VARCHAR2
		)
  IS


  ln_site_id 				NUMBER;
  ln_flex_value_id			NUMBER;
  ln_store_letter_count		NUMBER;
  lv_attr_group_type 			VARCHAR2(50)	       := 'RRS_SITEMGMT_GROUP';
  lv_application_short_name		VARCHAR2(10)	       := 'RRS';
  lv_ag_store_operator        	 	VARCHAR2(100)          := 'SLC_SM_STORE_OPERATOR';
  lv_ag_store_operator_pend		VARCHAR2(100)          := 'SLC_SM_STORE_OPERATOR_PRIOR';
  lv_err_flag		VARCHAR2(1)	 DEFAULT 'N';
  lv_err_msg		VARCHAR2(4000);
  lv_store_letter_code	rrs_sites_ext_b.c_ext_attr2%TYPE;
  lv_count		NUMBER;
  lv_found_attr_group_name		VARCHAR2(30);
  lv_store_effective_date		VARCHAR2(50);
  ln_program_status				NUMBER;
  lv_out_file_msg				VARCHAR2(4000);

   --Common error logging code.
  ln_total_record			NUMBER	DEFAULT 0;
  ln_total_success_records 		NUMBER DEFAULT 0;
  ln_total_failcust_validation		NUMBER DEFAULT 0;
  ln_total_errorcust_validation		NUMBER DEFAULT 0;
  lv_batch_status			VARCHAR2(1);
  lv_publish_flag			VARCHAR2(1);
  lv_system_type			VARCHAR2(10);
  lv_source				VARCHAR2(10);
  lv_destination			VARCHAR2(10);
  lv_cmn_err_status_code		VARCHAR2(100);
  lv_cmn_err_msg			VARCHAR2(1000);
  lv_business_process_id1		VARCHAR2(25) := NULL; --Reserved for Parent Record Id
  lv_business_process_id2		VARCHAR2(25) := NULL;  --Reserved for Child Record Id
  lv_business_process_id3		VARCHAR2(25) := NULL;
  lv_business_entity_name			  VARCHAR2(50) := 'SLC_ENRICH_BAL_SEG_P';

  CURSOR c_fnd_value(p_in_store_letter_code IN VARCHAR2)
  IS
  SELECT ffv.flex_value_id
    FROM fnd_flex_value_sets ffvs
         ,fnd_flex_values ffv
   WHERE ffvs.flex_value_set_id = ffv.flex_value_set_id
     AND ffvs.flex_value_set_name = 'SLCGL_BALANCING_SEGMENT'
     AND ffv.flex_value = p_in_store_letter_code;

  -- Cursor to fetch all valid values
  CURSOR c_valid_flex_values
  IS
  SELECT ffv.flex_value , ffv.flex_value_id
    FROM fnd_flex_value_sets ffvs
         ,fnd_flex_values ffv
   WHERE ffvs.flex_value_set_id = ffv.flex_value_set_id
     AND ffv.enabled_flag = 'Y'
     AND ffvs.flex_value_set_name = 'SLCGL_BALANCING_SEGMENT'
	 --Changes for v1.7.
	 --Earlier end_date_active and attribute14 is null was checked. But we need to check date effectivity
     --AND ffv.end_date_active IS NULL
     --AND ffv.attribute14 IS NULL
	 AND NVL(ffv.end_date_active,SYSDATE)>=SYSDATE
	 AND NVL(to_date(ffv.attribute14,'YYYYMMDD'),SYSDATE)>=SYSDATE
	 --This regular expression condition has been added to make sure that we pick only flex values of pattern
	 -- 5 digit followed by a character.
	 AND REGEXP_LIKE (ffv.flex_value, '^\d{5}\D$');

	 --Changes for v1.5 Begin
	 ld_actual_open_date		rrs_sites_ext_b.D_EXT_ATTR1%TYPE;
	 ld_permanent_close_date	rrs_sites_ext_b.D_EXT_ATTR1%TYPE;
	 ln_total_record_enriched			NUMBER DEFAULT 0;
     ln_total_record_enddated			NUMBER DEFAULT 0;
	 ln_total_enrich_enddate			NUMBER DEFAULT 0;
	 --Changes for v1.5 End

  BEGIN

    gv_debug_flag := p_debug_flag;
	ln_program_status := 0;
    slc_write_log_p(gv_log,'In slc_enrich_bal_seg_p. p_in_store_letter_code:'||p_in_store_letter_code);

	slc_write_log_p(gv_out,'*************************Output***************************');
    slc_write_log_p(gv_out,'*************************Parameters***************************');
    slc_write_log_p(gv_out,'p_in_store_letter_code: '||p_in_store_letter_code);
    slc_write_log_p(gv_out,'p_debug_flag: '||p_debug_flag);
	 slc_write_log_p(gv_out,'gn_request_id: '||gn_request_id);
    slc_write_log_p(gv_out,'**************************************************************');


    /*
     * This procedure will be called from 2 points.
     * 1. When the changeover execution has taken place and status of Site has been changed.
          In that program will call this for particular Site Number.
       2. This program will be scheduled daily. In that case no Site Number will be passed to the program.
     */
    IF p_in_store_letter_code IS NOT NULL THEN
      ln_total_record := ln_total_record + 1;

      	-- We need to look for Store Letter Code only in Active group as this program will be
      	-- trigged when the change over has been completed and Store has been moved to Active state.
		BEGIN
			SELECT c_ext_attr1 ,site_id
			  INTO lv_store_letter_code , ln_site_id
				FROM rrs_sites_ext_b rrs
				  ,ego_attr_groups_v eagv
				  ,fnd_application fnda
				WHERE rrs.attr_group_id         = eagv.attr_group_id
				AND eagv.attr_group_type        = lv_attr_group_type
				AND eagv.attr_group_name        = lv_ag_store_operator
				AND eagv.application_id         = fnda.application_id
				AND fnda.application_short_name = lv_application_short_name
				AND rrs.c_ext_attr1 = TRIM(leading '0' from p_in_store_letter_code);
		EXCEPTION
		WHEN NO_DATA_FOUND THEN
          lv_err_flag := 'Y';
          lv_err_msg  := 'Store Letter Code is not available in Active Operator group';
		WHEN OTHERS THEN
          lv_err_flag := 'Y';
          lv_err_msg  := 'Unexpected error while fetching Store Letter Code. Error Message: '||SQLERRM;
		END;

      slc_write_log_p(gv_log,'After Store Letter Code select. lv_err_flag:'||lv_err_flag||' lv_err_msg:'||lv_err_msg);
      slc_write_log_p(gv_log,'lv_store_letter_code:'||lv_store_letter_code||' ln_site_id:'||ln_site_id);


      --If there is no error while fetching Store Letter Code fetch information from fnd_flex_values.
      IF lv_err_flag = 'N' THEN

	    /* If a Store Letter Code is already existing then we will have to re-enable it. */
        OPEN c_fnd_value(lv_store_letter_code);
        FETCH c_fnd_value INTO ln_flex_value_id;
		CLOSE c_fnd_value;
          slc_write_log_p(gv_log,'ln_flex_value_id:'||ln_flex_value_id);

		  -- If Store Letter Code is already existing then update existing flex_value.
		  -- Else create new flex_value
          IF ln_flex_value_id IS NOT NULL THEN
			slc_load_flex_value_p(lv_store_letter_code,ln_site_id,'UPDATE','OVERRIDE',ln_flex_value_id,lv_err_flag,lv_err_msg);
			lv_out_file_msg := 'Existing Flex Value has been enabled again and DFF attributes has been enriched';
          ELSIF ln_flex_value_id IS NULL THEN
		  	 SELECT fnd_flex_values_s.NEXTVAL
			   INTO ln_flex_value_id
	           FROM dual;
            slc_load_flex_value_p(lv_store_letter_code,ln_site_id,'CREATE',NULL,ln_flex_value_id,lv_err_flag,lv_err_msg);
			lv_out_file_msg := 'New Flex Value has been created and DFF attributes has been enriched';
          END IF;



      END IF;

      --If there is error then log the error message.
      IF lv_err_flag = 'Y' THEN
     	slc_populate_err_object_p(p_in_batch_key => gv_batch_key
     			,p_in_business_entity => lv_business_entity_name
     			,p_in_process_id3 => lv_store_letter_code
     			,p_in_error_txt => lv_err_msg
     			,p_in_request_id => gn_request_id
     			,p_in_attribute1 => 'Store Letter Code:'||lv_store_letter_code
     			);
	  slc_write_log_p(gv_out,'Error while creating new value for Store Letter Code:'||lv_store_letter_code||
						' Error Message:'||lv_err_msg);
		ln_total_failcust_validation := ln_total_failcust_validation + 1;
		--ln_program_status := 2;
	  ELSIF lv_err_flag = 'N' THEN
	    ln_total_success_records	:= ln_total_success_records + 1;
	    slc_write_log_p(gv_out,lv_out_file_msg);
      END IF;


    ELSIF p_in_store_letter_code IS NULL THEN
     FOR c_valid_flex_rec IN c_valid_flex_values
     LOOP
       BEGIN
       slc_write_log_p(gv_log,'Store Number is null. Value :'||c_valid_flex_rec.flex_value||' Flex_value_id:'||
										c_valid_flex_rec.flex_value_id);
       lv_err_flag	:= 'N';
       lv_err_msg	:= NULL;
	   lv_store_letter_code := NULL;
	   ln_total_record := ln_total_record + 1;

	   --Changes for v1.5 Begin
	   ld_permanent_close_date := NULL;
	   ld_actual_open_date     := NULL;
	   --Changes for v1.5 End

	   	SELECT store_letter_code, site_id ,found_attr_group_name
	   	  INTO lv_store_letter_code, ln_site_id ,lv_found_attr_group_name
	   	  FROM (
	   	  	   SELECT rrs.c_ext_attr1 store_letter_code
	   	  	   	, rrs.site_id site_id
	   	  	   	, lv_ag_store_operator found_attr_group_name
			     FROM rrs_sites_ext_b rrs
				  ,ego_attr_groups_v eagv
				  ,fnd_application fnda
				WHERE rrs.attr_group_id         = eagv.attr_group_id
				AND eagv.attr_group_type        = lv_attr_group_type
				AND eagv.attr_group_name        = lv_ag_store_operator
				AND eagv.application_id         = fnda.application_id
				AND fnda.application_short_name = lv_application_short_name
				AND rrs.c_ext_attr1 = c_valid_flex_rec.flex_value
				--C_EXT_ATTR2 will store value of Store Code for Attribute Group SLC_SM_STORE_OPERATOR
			   UNION
			    SELECT rrs.c_ext_attr1 store_letter_code
			         , rrs.site_id site_id
			         , lv_ag_store_operator_pend found_attr_group_name
			     FROM rrs_sites_ext_b rrs
				  ,ego_attr_groups_v eagv
				  ,fnd_application fnda
				WHERE rrs.attr_group_id         = eagv.attr_group_id
				AND eagv.attr_group_type        = lv_attr_group_type
				AND eagv.attr_group_name        = lv_ag_store_operator_pend
				AND eagv.application_id         = fnda.application_id
				AND fnda.application_short_name = lv_application_short_name
				AND rrs.c_ext_attr1 = c_valid_flex_rec.flex_value
				--C_EXT_ATTR1 will store value of Store Code for Attribute Group SLC_SM_STORE_OPERATOR_PEND
	   	       );
	     slc_write_log_p(gv_log,'ln_site_id:'||ln_site_id||
	     		      ' lv_store_letter_code'||lv_store_letter_code||
	     		      ' lv_found_attr_group_name:'||lv_found_attr_group_name);

	     --we can remove this effective date parameter.
	     IF lv_found_attr_group_name = lv_ag_store_operator THEN
			slc_write_log_p(gv_log,'In enrich');
	        slc_load_flex_value_p(lv_store_letter_code,ln_site_id,'UPDATE','ENRICH',c_valid_flex_rec.flex_value_id,lv_err_flag,lv_err_msg);
			slc_write_log_p(gv_log,'In enrich after update lv_err_flag:'||lv_err_flag);

			--Changes for v1.5 Begin
		 IF lv_err_flag = 'N' THEN
			   BEGIN
					SELECT rrs.d_ext_attr1 ,rrs.d_ext_attr2
					 INTO ld_actual_open_date,ld_permanent_close_date
					 FROM rrs_sites_ext_b rrs
					  ,ego_attr_groups_v eagv
					  ,fnd_application fnda
					WHERE rrs.attr_group_id         = eagv.attr_group_id
					AND eagv.attr_group_type        = 'RRS_SITEMGMT_GROUP'
					AND eagv.attr_group_name        = 'SLC_SM_OPERATIONAL_DATE'
					AND eagv.application_id         = fnda.application_id
					AND fnda.application_short_name = 'RRS'
					AND rrs.site_id = ln_site_id;
					slc_write_log_p(gv_log,'In deactivate closed stores'||
											' ld_actual_open_date:'||to_char(ld_actual_open_date,'DD-MM-YYYY')||
											' ld_permanent_close_date:'||to_char(ld_permanent_close_date,'DD-MM-YYYY'));
				EXCEPTION
				WHEN OTHERS THEN
					slc_write_log_p(gv_log,'Exception while fetching Permanent Close Date');
					lv_err_flag := 'Y';
					lv_err_msg := lv_err_msg||'~Exception while fetching Permanent Close Date'||SQLERRM;
				END;
				slc_write_log_p(gv_log,'After fetching Permanent Close Date: lv_err_flag:'||lv_err_flag);
				IF lv_err_flag = 'N' THEN
					IF ((ld_permanent_close_date < SYSDATE) OR (ld_actual_open_date IS NULL AND ld_permanent_close_date IS NULL)) THEN
						update fnd_flex_values
						   set last_update_date = SYSDATE,
							   last_updated_by = gn_user_id,
							   attribute14 = to_char(ld_permanent_close_date, 'YYYYMMDD')
							   --Changes for v1.7.
							   --Uncommenting enabled_flag
							   --enabled_flag = 'N'
						 where flex_value = c_valid_flex_rec.flex_value;

						 --Incrementing count of records which has been encriched and enddated.
						 ln_total_enrich_enddate := ln_total_enrich_enddate + 1;
					 END IF;
				 END IF;
		  END IF;--End of Permanent Closed Date check.

			--Incrementing counts of records which has been enriched.
			 ln_total_record_enriched := ln_total_record_enriched + 1;
			--Changes for v1.5 End
		 --If the Store has been moved to History group or Prior group then updates its Store termination date
		 -- in the flex value
	     ELSIF lv_found_attr_group_name = lv_ag_store_operator_pend THEN
		 slc_write_log_p(gv_log,'In deactivate');
	       BEGIN
			    SELECT to_char(rrs.D_EXT_ATTR2  , 'YYYYMMDD') --MMDDYY...Changes expected.
				 INTO lv_store_effective_date
			     FROM rrs_sites_ext_b rrs
				  ,ego_attr_groups_v eagv
				  ,fnd_application fnda
				WHERE rrs.attr_group_id         = eagv.attr_group_id
				AND eagv.attr_group_type        = lv_attr_group_type
				AND eagv.attr_group_name        = lv_ag_store_operator_pend
				AND eagv.application_id         = fnda.application_id
				AND fnda.application_short_name = lv_application_short_name
				AND rrs.C_EXT_ATTR1 = c_valid_flex_rec.flex_value;
				slc_write_log_p(gv_log,'In deactivate lv_store_effective_date:'||lv_store_effective_date);

			update fnd_flex_values
			   set last_update_date = SYSDATE,
                   last_updated_by = gn_user_id,
                   attribute14 = lv_store_effective_date
				   --Changes for v1.5. When we want to enddate Store Letter Code which has been
				   --present in Prior page in Site Hub we need to set enabled_flag as well.
				   --Changes for v1.7.
				   --Uncommenting enabled_flag
				   --enabled_flag = 'N'
			 where flex_value = c_valid_flex_rec.flex_value;
			ln_total_record_enddated := ln_total_record_enddated + 1;
			COMMIT;

		   EXCEPTION
		   WHEN OTHERS THEN
			lv_err_flag := 'Y';
			lv_err_msg := lv_err_msg||'~Error while updating Store Termination Date: Store Code:'||c_valid_flex_rec.flex_value;
		   END;
	     END IF;

	   EXCEPTION
	   WHEN NO_DATA_FOUND THEN
		lv_err_flag := 'Y';
		lv_err_msg := lv_err_msg||'~Store Letter Code not found in Site Hub: Store Code:'||c_valid_flex_rec.flex_value;
	   WHEN TOO_MANY_ROWS THEN
		lv_err_flag := 'Y';
		lv_err_msg := lv_err_msg||'~Multiple Sites have same Store Letter Code or Store Letter Code present in multiple Attribute Groups.'||
								  'Store Code:'||c_valid_flex_rec.flex_value;
	   WHEN OTHERS THEN
		lv_err_flag := 'Y';
		lv_err_msg := lv_err_msg||'~Unexpected error while fetching information for Store Code:'
				||c_valid_flex_rec.flex_value||' Error Message:'||SQLERRM;
	   END;
	     slc_write_log_p(gv_log,'After enriching attributes: lv_err_flag:'||lv_err_flag||' lv_err_msg:'||lv_err_msg);

	   --If there is error then log the error message.
	   IF lv_err_flag = 'Y' THEN
	     ln_total_failcust_validation := ln_total_failcust_validation + 1;
		 slc_write_log_p(gv_out,'Error while enriching flex value.'||
						  ' Error Message:'||lv_err_msg);
     	slc_populate_err_object_p(p_in_batch_key => gv_batch_key
     			,p_in_business_entity => lv_business_entity_name
     			,p_in_process_id3 => NULL
     			,p_in_error_txt => lv_err_msg
     			,p_in_request_id => gn_request_id
     			,p_in_attribute1 => 'Store Letter Code:'||c_valid_flex_rec.flex_value
     			);
		  --ln_program_status := 1;
		ELSIF lv_err_flag = 'N' THEN
		  ln_total_success_records	:= ln_total_success_records + 1;
	    END IF;


     END LOOP;

	 slc_write_log_p(gv_out,'*********************************************************************');
	 slc_write_log_p(gv_out,'***************************SUMMARY  START*****************************');
	 slc_write_log_p(gv_out,rpad('Total records picked:',80,' ')||(ln_total_success_records+ln_total_failcust_validation));
	 --slc_write_log_p(gv_out,'Total records enriched:'||ln_total_success_records);
	 slc_write_log_p(gv_out,rpad('Total records Enriched:',80,' ')||ln_total_record_enriched);
	 slc_write_log_p(gv_out,rpad('Total records Endriched and Enddated:',80,' ')||ln_total_enrich_enddate);
	 slc_write_log_p(gv_out,rpad('Total records Enddated as it was in Prior page in Site Hub:',80,' ')||ln_total_record_enddated);
	 slc_write_log_p(gv_out,rpad('Total records Failed while enriching:',80,' ')||ln_total_failcust_validation);
	 slc_write_log_p(gv_out,'***************************SUMMARY  END*****************************');
	 slc_write_log_p(gv_out,'*********************************************************************');

    END IF;
	slc_write_log_p(gv_log,'Final ln_total_success_records:'||ln_total_success_records||' ln_total_failcust_validation:'||
							ln_total_failcust_validation);
	IF ln_total_failcust_validation = 0 THEN
		ln_program_status := 0;
	ELSIF ln_total_failcust_validation <> ln_total_record THEN
		ln_program_status := 1;
	ELSIF ln_total_failcust_validation = ln_total_record THEN
		ln_program_status := 2;
	END IF;
   SLC_UTIL_JOBS_PKG.SLC_UTIL_E_LOG_SUMMARY_P(
							P_BATCH_KEY => gv_batch_key,
							P_BUSINESS_PROCESS_NAME => gv_business_process_name,
							P_TOTAL_RECORDS => ln_total_record,
							P_TOTAL_SUCCESS_RECORDS => ln_total_success_records,
							P_TOTAL_FAILCUSTVAL_RECORDS => ln_total_failcust_validation,
							P_TOTAL_FAILSTDVAL_RECORDS => ln_total_errorcust_validation,
							p_batch_status  => lv_batch_status,
							p_publish_flag => lv_publish_flag,
							p_system_type => lv_system_type,
							p_source_system	=> lv_source,
							p_target_system => lv_destination,
							 P_REQUEST_ID => gn_request_id,
							 p_user_id => gn_user_id,
							 p_login_id => gn_login_id,
							 p_status_code  => lv_cmn_err_status_code
							);

	SLC_UTIL_JOBS_PKG.slc_UTIL_log_errors_p(p_batch_key => gv_batch_key,
   					      p_business_process_name => gv_business_process_name,
   						  p_errors_rec => gv_cmn_err_rec,
   					      p_user_id => gn_user_id,
   					      p_login_id => gn_login_id,
   					      p_status_code  => lv_cmn_err_status_code
   					     );

	p_retcode := ln_program_status ;
  END slc_enrich_bal_seg_p;

END slc_glext_bal_seg_pkg;


/
SHOW ERROR
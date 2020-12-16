WITH l3group AS (SELECT DISTINCT a.process_area_l1 l1,
								 a.process_area_l2 l2,
								(SELECT b.decision_name decisionname,
										b.decision_impact decisionimpact,
										b.decision_description description,
										b.decision_name decisionelinkname,
										(SELECT CONCAT(b1.doc_link, '/', b1.file_name)
										   FROM ASCEND.documents b1
										  WHERE b.decision_name = b1.name
										    AND b1.type = 'KDD') decisiondoclink,
										(CASE
											WHEN ((SELECT COUNT(1)
													FROM ASCEND.project_workspace pws
														,ASCEND.user_roles ur
													WHERE ur.ID = @userroleid
													and ur.project_id = pws.project_id
													AND b.decision_name = pws.entity_value
													AND b.process_area_l2 = pws.L2
													AND b.process_area_l1 = pws.L3) = 1)
											THEN 'Y' else 'N'
											END) decisionenabledFlag
								   FROM ASCEND.key_business_decision_new b
								  WHERE a.process_area_l1 = b.process_area_l1
								    AND a.process_area_l2 = b.process_area_l2
								    FOR JSON PATH, INCLUDE_NULL_VALUES) AS l3grp
				FROM ASCEND.key_business_decision_new a
			   WHERE 1=1
				 AND (ISNULL(a.industry_id,1) = 1 OR a.industry_id IN (SELECT VALUE FROM STRING_SPLIT(@industry, ',')))
				 AND (ISNULL(a.sector_id,1) = 1 OR a.sector_id IN (SELECT VALUE FROM STRING_SPLIT(@sector, ',')))
				 AND (ISNULL(a.region_id,1) = 1 OR a.region_id IN (SELECT VALUE FROM STRING_SPLIT(@region, ',')))
				 AND (ISNULL(a.l1_id,1) = 1 OR a.l1_id IN (SELECT VALUE FROM STRING_SPLIT(@l1, ',')))
				 AND (ISNULL(a.l2_id,1) = 1 OR a.l2_id IN (SELECT VALUE FROM STRING_SPLIT(@l2, ',')))
				 AND (ISNULL(a.l3_id,1) = 1 OR a.l3_id IN (SELECT VALUE FROM STRING_SPLIT(@l3, ',')))),
     l2group AS  (SELECT DISTINCT c.l1 L1value,
								  (SELECT d.l2 L2value,
										  d.l2 L2linkname,
										  (SELECT CONCAT(b1.doc_link, '/', b1.file_name)
											 FROM ASCEND.documents b1
											WHERE d.l2 = b1.name
											  AND b1.type = 'KDD') L2doclink,
										  (CASE
											  WHEN ((SELECT COUNT(1)
														FROM ASCEND.project_workspace pws
															,ASCEND.user_roles ur
														WHERE ur.ID = @userroleid
														and ur.project_id = pws.project_id
														AND d.l2 = pws.L2
														AND d.l1 = pws.L3) = 1)
											  THEN 'Y' else 'N'
											  END) L2enabledflag,
										 d.l3grp
									 FROM l3group d
									WHERE c.l1 = d.l1
									  FOR JSON PATH, INCLUDE_NULL_VALUES) AS l2grp
				 FROM l3group c),
	t(data) AS (SELECT e.L1value L1value,
					   (SELECT CONCAT(b1.doc_link, '/', b1.file_name)
						  FROM ASCEND.documents b1
						 WHERE e.L1value = b1.name
						   AND b1.type = 'KDD') L1doclink,
					   CONCAT('/assets/documents/', e.L1value) L1linkname,
					   (CASE WHEN ((SELECT COUNT(1)
									FROM ASCEND.project_workspace pws
										,ASCEND.user_roles ur
									WHERE ur.ID = @userroleid
									AND ur.project_id = pws.project_id
									AND e.L1value = pws.L3) = 1)
							 THEN 'Y' else 'N'
							 END) L1enabledflag,
						e.l2grp
				FROM l2group e
				FOR JSON PATH, INCLUDE_NULL_VALUES)
SELECT data FROM t
 
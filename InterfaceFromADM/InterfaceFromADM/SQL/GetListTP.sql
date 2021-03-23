
delete from TB_T_TRANSFER_POSTING_TO_ICS
DECLARE @@dockServicePartTable AS TABLE (DOCK VARCHAR(2));
DECLARE @@IS_VALID_FILTER CHAR(1);

INSERT INTO @@dockServicePartTable (DOCK) EXEC dbo.SplitString @dockServicePart, ','

IF ( @FLTR_FLAG_VALUE = 'Y' ) 
BEGIN                                                                                                        
    IF ( @FLTR_LOGICAL_VALUE IN ( '=', '>', '<', '>=', '<=' ) )
        AND ( LEN(@FLTR_DATETIME_VALUE) = 14 )
        BEGIN
            SET @@IS_VALID_FILTER = 'Y'                                                                                                  
        END 
    ELSE 
        BEGIN
            SET @@IS_VALID_FILTER = 'N'                                                                                                  
        END
END                                                                                                          
ELSE 
BEGIN                                                                                                        
    IF ( @FLTR_FLAG_VALUE = 'N' ) 
        SET @@IS_VALID_FILTER = 'Y'
    ELSE 
        SET @@IS_VALID_FILTER = 'N'                                                                                                      
END

IF ( @@IS_VALID_FILTER = 'Y' ) 
BEGIN

--TP  
insert into TB_T_TRANSFER_POSTING_TO_ICS  
select CAST(@PID as varchar(20)) as PROCESS_ID,
	   @SYS_SOURCE + MANIFEST_NO + PART_NO+ PROD_PURPOSE_CD+ SOURCE_TYPE as PROCESS_KEY,
	   @SYS_SOURCE as SYSTEM_SOURCE,
	   @clientId as CLIENT_ID,
	   MOVE_TYPE as MOV_TYPE,
	   CONVERT(VARCHAR(10),getdate(),104) AS DOC_DT, -- change format to dd.mm.yyyy
	   CONVERT(VARCHAR(10),CONVERT(DATE,CONVERT(VARCHAR(MAX),DROP_STATUS_DATE,112)),104)  AS POSTING_DT, -- change format to dd.mm.yyyy
	   MANIFEST_NO as REF_NO,
	   @MAT_DOC_DESC as MAT_DOC_DESC,
	   PART_NO as SND_PART_NO,
	   PROD_PURPOSE_CD as SND_PROD_PURPOSE_CD,
	   SOURCE_TYPE as SND_SOURCE_TYPE,  -- select * from TB_M_DOCK_ICS where '2020-10-31 00:00:00.000' between VALID_DT_FR and VALID_DT_TO
	   (select PLANT_CD from TB_M_DOCK_ICS where DOCK_CD = SHIPPING_DOCK and DROP_STATUS_DATE between VALID_DT_FR and VALID_DT_TO ) as SND_PLANT_CD,   --- SP1
	   (select SLOC_CD from TB_M_DOCK_ICS where DOCK_CD = SHIPPING_DOCK and DROP_STATUS_DATE between VALID_DT_FR and VALID_DT_TO) as SND_SLOC_CD,
	   '' as SND_BATCH_NO,
	   PART_NO as RCV_PART_NO,
	   PROD_PURPOSE_CD as RCV_PROD_PURPOSE_CD,
	   SOURCE_TYPE as RCV_SOURCE_TYPE,
	   (select PLANT_CD from TB_M_DOCK_ICS where DOCK_CD = DOCK_CODE and DROP_STATUS_DATE between VALID_DT_FR and VALID_DT_TO) as RCV_PLANT_CD,       -- 43
	   (select SLOC_CD from TB_M_DOCK_ICS where DOCK_CD = DOCK_CODE and DROP_STATUS_DATE between VALID_DT_FR and VALID_DT_TO) as RCV_SLOC_CD,
	   '' as RCV_BATCH_NO,
	   CAST(RECEIVED_QTY AS VARCHAR) as QUANTITY,
	   @URI_UOM as UNIT_OF_MEASURE_CD,
	   'Y' DN_COMPLETE_FLAG, -- default value =Y
	   'ippcs.admin' as CREATED_BY,
	   CONVERT(VARCHAR(10),getdate(),104) +' '+ convert(varchar, getdate(), 108)  as CREATED_DT -- change format to DD.MM.YYYY HH:MM:SS

from (                                                                                        
SELECT A.MANIFEST_NO
	   ,A.SUPPLIER_CODE
	   ,@SOURCE_TYPE_TP SOURCE_TYPE
	   ,A.SUPPLIER_PLANT
	   ,A.SHIPPING_DOCK
	   ,CASE WHEN (SELECT 1 WHERE A.DOCK_CODE IN (SELECT DOCK FROM @@dockServicePartTable)) = 1 
	   THEN @prodPurposeServicePart
	   ELSE
	   @PROD_PURPOSE_CD
	   END
	   AS PROD_PURPOSE_CD
	   ,A.COMPANY_CD
	   ,A.RCV_PLANT_CODE
	   ,A.DOCK_CODE
	   ,A.ORDER_NO
	   ,A.CAR_FAMILY_CD
	   ,A.RE_EXPORT_CD
	   ,A.PART_NO
	   ,'' AS KNBN_PRN_ADDRESS
	   ,A.ORDER_TYPE
	   ,A.MOVE_TYPE
	   ,A.DROP_STATUS_DATE
	   ,A.RECEIVED_QTY
	   ,A.DN_COMPLETE_FLAG
	   ,A.DATA_TYPE
	   ,A.ORDER_RELEASE_DT
FROM ( 
SELECT A.MANIFEST_NO
	   ,A.SUPPLIER_CODE
	   ,A.SUPPLIER_PLANT
	   ,A.SHIPPING_DOCK
	   ,A.COMPANY_CD
	   ,A.RCV_PLANT_CODE
	   ,A.DOCK_CODE
	   ,A.ORDER_NO
	   ,A.CAR_FAMILY_CD
	   ,A.RE_EXPORT_CD
	   ,A.PART_NO
	   ,'' AS KNBN_PRN_ADDRESS
	   ,A.ORDER_TYPE
	   ,CASE LEFT(A.SUPPLIER_CODE, 3)
	   WHEN '807'
	   THEN CASE 
	   WHEN (
	   SELECT 1
	     WHERE A.DOCK_CODE IN (
	   SELECT DOCK
	   FROM @@dockServicePartTable
)
) = 1
THEN (
	   SELECT SYSTEM_VALUE
	   FROM TB_M_SYSTEM
	   WHERE SYSTEM_CD = 'MOVEMENT_TYPE_SERVICE_PART'
)
ELSE '301'
END
ELSE '101'
END AS MOVE_TYPE
	   ,MAX(A.DROP_STATUS_DT) AS DROP_STATUS_DATE
	   ,SUM(cast(A.ORDER_QTY as float)) AS RECEIVED_QTY
	   ,DBO.GET_VALUE_FROM_M_SYSTEM(@TYPE_CODE, 'DN_UNCOMPLETE_FLG_GR') AS DN_COMPLETE_FLAG
	   ,'TP' AS DATA_TYPE
	   ,A.ORDER_RELEASE_DT
FROM (
SELECT DISTINCT (c.MANIFEST_NO)
	   ,c.SUPPLIER_CD AS SUPPLIER_CODE
	   ,c.SUPPLIER_PLANT
	   ,c.SHIPPING_DOCK
	   ,c.RCV_PLANT_CD AS RCV_PLANT_CODE
	   ,c.DOCK_CD AS DOCK_CODE
	   ,c.ICS_FLAG
	   ,SUM(b.ORDER_QTY) ORDER_QTY
	   ,c.ORDER_NO
	   ,b.PART_NO
	   ,c.DROP_STATUS_FLAG
	   ,c.DROP_STATUS_DT
	   ,b.KANBAN_PRINT_ADDRESS AS KNBN_PRN_ADDRESS
	   ,c.ORDER_TYPE
	   ,C.COMPANY_CD
	   ,C.CAR_FAMILY_CD
	   ,C.RE_EXPORT_CD
	   ,CASE LEFT(c.SUPPLIER_CD, 3)
	   WHEN '807'
	   THEN CASE 
	   WHEN (
	   SELECT 1
	   WHERE c.DOCK_CD IN (
	   SELECT DOCK
	   FROM @@dockServicePartTable
	   )
	   ) = 1
	   THEN (
	   SELECT SYSTEM_VALUE
	   FROM TB_M_SYSTEM
	   WHERE SYSTEM_CD = 'MOVEMENT_TYPE_SERVICE_PART'
	   )
	   ELSE '301'
	   END
	   ELSE '101'
	   END AS MOVE_TYPE
	   ,dbo.GET_VALUE_FROM_M_SYSTEM(@TYPE_CODE, 'DN_UNCOMPLETE_FLG_GR') AS DN_COMPLETE_FLAG
	   ,'TP' AS DATA_TYPE
	   ,c.ARRIVAL_PLAN_DT AS ORDER_RELEASE_DT
FROM TB_R_DAILY_ORDER_MANIFEST c
INNER JOIN (
SELECT MANIFEST_NO
	  ,SUM(cast(ORDER_QTY as float)) ORDER_QTY
	   ,RCV_PLANT_CD
	   ,PART_NO
	   ,KANBAN_PRINT_ADDRESS
	   ,POSTING_STS
FROM TB_R_DAILY_ORDER_PART
WHERE case when POSTING_STS is null or convert(varchar(max),POSTING_STS) = 'NULL'  then 3 else POSTING_STS end <> 0
AND ORDER_QTY > 0

GROUP BY (MANIFEST_NO)
,PART_NO
,RCV_PLANT_CD
,PART_NO
,KANBAN_PRINT_ADDRESS
,POSTING_STS
) AS b ON c.MANIFEST_NO = b.MANIFEST_NO
 WHERE c.PROBLEM_FLAG = '0'
	  AND c.CANCEL_FLAG = '0'
AND c.MANIFEST_RECEIVE_FLAG IN ('2','3','5')
AND c.SUPPLIER_CD LIKE '%' + @TP_SUPPLIER_CODE + '%'
GROUP BY c.MANIFEST_NO
		 ,SUPPLIER_CD
		 ,SUPPLIER_PLANT
		 ,c.SHIPPING_DOCK
		 ,c.RCV_PLANT_CD
		 ,DOCK_CD
		 ,ORDER_NO
		 ,PART_NO
		 ,c.DROP_STATUS_FLAG
		 ,c.DROP_STATUS_DT
		 ,b.KANBAN_PRINT_ADDRESS
		 ,ORDER_TYPE
		 ,C.COMPANY_CD
		 ,C.CAR_FAMILY_CD
		 ,C.RE_EXPORT_CD
		 ,c.ICS_FLAG
		 ,c.ARRIVAL_PLAN_DT
) AS A
WHERE 1=1
GROUP BY A.MANIFEST_NO
		 ,A.SUPPLIER_CODE
		 ,A.SUPPLIER_PLANT
		 ,A.SHIPPING_DOCK
		 ,A.COMPANY_CD
		 ,A.RCV_PLANT_CODE
		 ,A.DOCK_CODE
		 ,A.ORDER_NO
		 ,A.CAR_FAMILY_CD
		 ,A.RE_EXPORT_CD
		 ,A.PART_NO
		 ,A.ORDER_TYPE
		 ,A.ORDER_RELEASE_DT
) A      
)B
END

select   * from TB_T_TRANSFER_POSTING_TO_ICS
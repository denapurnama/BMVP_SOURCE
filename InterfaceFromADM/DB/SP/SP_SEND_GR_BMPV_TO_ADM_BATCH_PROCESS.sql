CREATE PROCEDURE [dbo].[SP_SEND_GR_BMPV_TO_ADM_BATCH_PROCESS]
	@ri_n_process_id		bigint,
	@ri_n_err				int OUTPUT,
	@ri_v_function_id		varchar(10),
	@ri_v_module_id			varchar(10),
	@ri_v_user_id			varchar(20),
	@ri_n_is_empty			int OUTPUT
AS
BEGIN
	IF OBJECT_ID('tempdb..#temp') IS NOT NULL
		DROP TABLE #temp

	IF OBJECT_ID('tempdb..#temp_loop') IS NOT NULL
		DROP TABLE #temp_loop

	DECLARE
		@l_v_sql_queries		varchar(max) = '',
		@l_v_file_name			varchar(MAX) = '',
		@l_v_substr_file_name	varchar(MAX) = '',
		@l_v_log_position		varchar(max) = 'SendGRBMPVtoADM.GenerateData',
		@l_v_interface_name		varchar(100) = 'BMPV_ADM_GOOD_RECEIPT', 
		@l_v_file_id			varchar(100) = (SELECT SYSTEM_VALUE FROM TB_M_SYSTEM WHERE FUNCTION_ID ='GOOD_RECEIPT_BMPV' AND SYSTEM_CD ='GOODS_RECEIPT_FILE_NAME_BMPV')  ,
		@l_v_supp_cd			varchar(max),
		@l_v_file_date			varchar(max), 
		@supp_cd				VARCHAR(50), 
		@l_n_row_count			int = 0,
		@l_n_process_status		tinyint = 0,
		@l_v_log_mesg			varchar(max),
		@l_n_max_loop			int = 0,
		@l_n_index_loop			int = 1,
		@l_v_rcv_no				varchar(10),
		@l_v_po_tam				varchar(16),
		@l_v_supplier_tam		varchar(6),
		@l_v_part_no			varchar(12),
		@l_v_bining_dt			varchar(100),
		@l_n_bining_qty			int,
		@l_n_error_loop			int = 0,
		@l_n_send_record		int = 0,
		@l_n_po_adm_success		int = 0

	BEGIN TRY
			DELETE FROM TB_T_BMPV_GR_DATA 
			SET @supp_cd= (SELECT SYSTEM_VALUE FROM TB_M_SYSTEM WHERE FUNCTION_ID = 'SUPPLIER_CD_ADM_BMPV') 


			--DATA ORDER
			BEGIN TRY
				INSERT INTO TB_T_BMPV_GR_DATA
				SELECT  
				 TRDOM.MANIFEST_NO  
				 ,TRDOM.DOCK_CD 
				 ,TRDOP.PART_NO  
				 ,TRDOP.PART_NAME  
				 ,TRDOP.QTY_PER_CONTAINER  
				 ,TRDOP.ORDER_QTY  
				 ,CONVERT(CHAR(19),CONVERT(DATETIME,TRDOM.ARRIVAL_PLAN_DT ,101),120) 
				 ,NULL
				 ,TMSC.SUPPLIER_CODE_TMMIN  
				 ,TMSC.SUPPLIER_PLANT_TMMIN  
				 ,TMS.SUPPLIER_NAME  
				 ,TMSC.SUPPLIER_CODE_ADM  
				 ,TMSC.SUPPLIER_PLANT_ADM  
				     
				FROM   
				 [TB_R_DAILY_ORDER_MANIFEST] TRDOM  
				 INNER JOIN [TB_R_DAILY_ORDER_PART] TRDOP ON TRDOP.MANIFEST_NO = TRDOM.MANIFEST_NO  
				 INNER JOIN [TB_M_SUPPLIER_CONVERSION] TMSC ON TMSC.SUPPLIER_CODE_TMMIN = TRDOM.SUPPLIER_CD AND TMSC.SUPPLIER_PLANT_TMMIN = TRDOM.SUPPLIER_PLANT  
				 INNER JOIN [TB_M_SUPPLIER] TMS ON TMS.SUPPLIER_CODE = TMSC.SUPPLIER_CODE_TMMIN AND TRDOM.SUPPLIER_PLANT=TMS.SUPPLIER_PLANT_CD
				WHERE  
				 TRDOM.SUPPLIER_CD IN((SELECT  * FROM DBO.FN_SPLIT(@supp_cd,',')))   
				 AND TRDOM.MANIFEST_RECEIVE_FLAG >0
				 AND (TRDOM.SEND_GR_TO_ADM_FLAG is null or TRDOM.SEND_GR_TO_ADM_FLAG = '')  and TRDOM.TOTAL_QTY<>'0' AND TRDOP.ORDER_QTY >0
			END TRY
			BEGIN CATCH 

				EXEC SP_GET_MESSAGE 'MPCS00008ERR', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @l_n_row_count, 'ERROR Send Data Order'
				EXEC SP_PUTLOG 'Error Found when send Data Order', @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MPCS00008ERR', 'ERR', @ri_v_module_id, @ri_v_function_id, @l_n_process_status
				return @l_n_process_status
			END CATCH
			 
			 IF NOT EXISTS (SELECT * FROM TB_T_BMPV_GR_DATA)
			 BEGIN
				EXEC SP_GET_MESSAGE 'MPCS00008ERR', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @l_n_row_count, 'Data Order not Found'
				EXEC SP_PUTLOG 'Data Order not found, cannot send Data GR', @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MPCS00008ERR', 'ERR', @ri_v_module_id, @ri_v_function_id, @l_n_process_status
				return @l_n_process_status
			END


			-- DATA GR
			INSERT INTO TB_T_BMPV_GR_DATA
			SELECT  
			 TRDOM.MANIFEST_NO  
			 ,TRDOM.DOCK_CD  
			 ,TRDOP.PART_NO  
			 ,TRDOP.PART_NAME  
			 ,TRDOP.QTY_PER_CONTAINER  
			 ,TRDOP.ORDER_QTY  
			 ,CONVERT(CHAR(19),CONVERT(DATETIME,TRDOM.ARRIVAL_PLAN_DT ,101),120) 
			 ,CONVERT(CHAR(19),CONVERT(DATETIME,TRDOM.ARRIVAL_ACTUAL_DT   ,101),120) 
			 ,TMSC.SUPPLIER_CODE_TMMIN  
			 ,TMSC.SUPPLIER_PLANT_TMMIN  
			 ,TMS.SUPPLIER_NAME  
			 ,TMSC.SUPPLIER_CODE_ADM  
			 ,TMSC.SUPPLIER_PLANT_ADM  
			     
			FROM   
			 [TB_R_DAILY_ORDER_MANIFEST] TRDOM  
			 INNER JOIN [TB_R_DAILY_ORDER_PART] TRDOP ON TRDOP.MANIFEST_NO = TRDOM.MANIFEST_NO  
			 INNER JOIN [TB_M_SUPPLIER_CONVERSION] TMSC ON TMSC.SUPPLIER_CODE_TMMIN = TRDOM.SUPPLIER_CD AND TMSC.SUPPLIER_PLANT_TMMIN = TRDOM.SUPPLIER_PLANT  
			 INNER JOIN [TB_M_SUPPLIER] TMS ON TMS.SUPPLIER_CODE = TMSC.SUPPLIER_CODE_TMMIN AND TRDOM.SUPPLIER_PLANT=TMS.SUPPLIER_PLANT_CD
			WHERE  
			 TRDOM.SUPPLIER_CD IN((SELECT  * FROM DBO.FN_SPLIT(@supp_cd,',')))   
			 AND TRDOM.MANIFEST_RECEIVE_FLAG >1
			 AND TRDOM.SEND_GR_TO_ADM_FLAG = '1'  and TRDOM.TOTAL_QTY<>'0' AND TRDOP.ORDER_QTY >0
  
		SELECT @l_n_row_count = COUNT(1) FROM TB_T_BMPV_GR_DATA --WHERE ISNULL(CAST(VALIDATION_STS, '') = 'OK'
		EXEC SP_GET_MESSAGE 'MSPXPR020INF', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @l_n_row_count, 'OK'
		EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR020INF', 'INF', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;

		IF(EXISTS(SELECT 'x' FROM TB_T_BMPV_GR_DATA))
		BEGIN
			EXEC SP_GET_MESSAGE 'MSPXPR008INF', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT
			EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR008INF', 'INF', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;
		
			SELECT @l_v_file_name = [FILE_ID] FROM TB_M_INTERFACE_FILE WHERE INTERFACE_NAME = @l_v_interface_name
			SELECT @l_v_substr_file_name = CASE WHEN LEN(@l_v_file_name) > 8 THEN LEFT(@l_v_file_name, 8) ELSE @l_v_file_name END
			SELECT @l_n_row_count = COUNT(1) FROM TB_T_BMPV_GR_DATA 
			SELECT @l_v_file_date = CONVERT(VARCHAR(10), GETDATE(), 112) + REPLACE(CONVERT(VARCHAR(10), GETDATE(), 108), ':', '')
			INSERT INTO TB_T_GR_BMPV_TO_ADM_INTERFACE (FUNCTION_ID, PROCESS_ID, [STAGE], [DATA])
			SELECT
				@ri_v_function_id,
				@ri_n_process_id,
				[STAGE], 
				[DATA]
			FROM (
				SELECT 1 [STAGE], '##H##' + '0' + SPACE(5) + @l_v_substr_file_name + @l_v_file_date + SPACE(99) + 
								 RIGHT(REPLICATE('0', 10) + CONVERT(VARCHAR(MAX), @l_n_row_count), 10) + SPACE(58) DATA
				UNION ALL
				SELECT
					2 [STAGE], 
					  
					 ISNULL(CAST(MANIFEST_NO AS VARCHAR),'')			+ CHAR(9) +
					 ISNULL(CAST(ORDER_NO AS VARCHAR),'')			+ CHAR(9) +
					 ISNULL(CAST(PART_NO AS VARCHAR),'')				+ CHAR(9) +
					 ISNULL(CAST(PART_NAME AS VARCHAR),'')			+ CHAR(9) +
					 ISNULL(CAST(QTY_PER_CONTAINER AS VARCHAR),'')   + CHAR(9) +
					 ISNULL(CAST(ORDER_QTY AS VARCHAR),'')			+ CHAR(9) +
					 ISNULL(CAST(ARRIVAL_PLAN_DT AS VARCHAR),'')		+ CHAR(9) +
					 ISNULL(CAST(ARRIVAL_ACTUAL_DT AS VARCHAR),'')   + CHAR(9) +
					 ISNULL(CAST(SUPPLIER_CODE_TMMIN AS VARCHAR),'') + CHAR(9) +
					 ISNULL(CAST(SUPPLIER_PLANT_TMMIN AS VARCHAR),'')   + CHAR(9) +
					 ISNULL(CAST(SUPPLIER_NAME AS VARCHAR),'')		+ CHAR(9) +
					 ISNULL(CAST(SUPPLIER_CODE_ADM AS VARCHAR),'')   + CHAR(9) +
					 ISNULL(CAST(SUPPLIER_PLANT_ADM AS VARCHAR),'')      [DATA]
					FROM  TB_T_BMPV_GR_DATA    

				--UNION
				--SELECT 3 STAGE, '#' + 'T' + '#' + @l_n_row_count + '#' DATA
			)TBL
			ORDER BY STAGE ASC

			SET @l_v_sql_queries = '"SELECT DATA FROM TB_T_GR_BMPV_TO_ADM_INTERFACE WHERE PROCESS_ID = ' + ISNULL(CONVERT(VARCHAR(MAX), @ri_n_process_id), '#') + ' AND FUNCTION_ID = ''' + ISNULL(CONVERT(VARCHAR(100), @ri_v_function_id), '#') + ''' ORDER BY STAGE ASC"'
		
			EXEC [SP_SEND_GR_BMPV_TO_ADM_BATCH_CREATE_FILE] @ri_n_process_id, @l_v_file_name, @l_v_interface_name, @ri_v_module_id, @ri_v_function_id, @ri_v_user_id, @l_v_sql_queries, @ri_n_err OUTPUT, ''
		
			IF(@ri_n_err = 0)
			--BEGIN
			--	UPDATE TB_T_BMPV_GR_DATA 
			--	SET SEND_ADM_STS = 'Y', CHANGED_BY = @ri_v_user_id, CHANGED_DT = GETDATE()
			--	WHERE ISNULL(CAST(VALIDATION_STS, '') = 'OK' AND ISNULL(CAST(SEND_ADM_STS, '') = 'N'
			--	SELECT @l_n_send_record = @@ROWCOUNT

			--	SELECT @l_n_po_adm_success = COUNT(DISTINCT PO_TAM) 
			--	FROM TB_T_BMPV_GR_DATA WHERE SEND_ADM_STS = 'Y'

				EXEC SP_GET_MESSAGE 'MSPXPR014INF', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @l_n_send_record, @l_n_po_adm_success
				EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR014INF', 'INF', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;
			
				UPDATE TB_R_DAILY_ORDER_MANIFEST  SET SEND_GR_TO_ADM_FLAG = 'YES'  
				 WHERE MANIFEST_NO IN (SELECT DISTINCT MANIFEST_NO FROM TB_T_BMPV_GR_DATA)  
				 
				SELECT @l_n_row_count = @@ROWCOUNT
			
				EXEC SP_GET_MESSAGE 'MSPXPR017INF', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @l_n_row_count
				EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR017INF', 'INF', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;

			
		END
		ELSE
		BEGIN
			EXEC SP_GET_MESSAGE 'MSPXPR008ERR', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT
			EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR008ERR', 'ERR', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;
			SET @ri_n_err = ISNULL(@ri_n_err, 0) + 1
			SET @ri_n_is_empty = ISNULL(@ri_n_is_empty, 0) + 1
		END
	END TRY
	BEGIN CATCH
		SET @l_v_log_mesg = ERROR_MESSAGE()
		--RAISERROR(@l_v_log_mesg, 16, 1)
		EXEC SP_GET_MESSAGE 'MSPXPR001ERR', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @l_v_log_mesg
		EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR001ERR', 'ERR', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;
		SET @ri_n_err = ISNULL(@ri_n_err, 0) + 1
	END CATCH
END



GO



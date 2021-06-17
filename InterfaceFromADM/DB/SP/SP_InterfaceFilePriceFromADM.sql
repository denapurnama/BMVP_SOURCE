USE [IPPCS_QA]
GO

/****** Object:  StoredProcedure [dbo].[SP_InterfaceFilePriceFromADM]    Script Date: 6/17/2021 8:58:18 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


-- =============================================
-- Author:		WOT-Daniar
-- Create date: 2021-01-20
-- Description:	Interface File Price From ADM Batch
-- =============================================
CREATE PROCEDURE [dbo].[SP_InterfaceFilePriceFromADM]
	-- Add the parameters for the stored procedure here
		--@ro_i_retval int OUTPUT,
		@PROCESS_ID bigint,
		@ro_v_err_mesg varchar(max) output

AS
BEGIN 
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	declare 
		@LogSeq int,
		@FUNC_ID varchar(max), 
		@N_ERR VARCHAR(MAX),
		@MODULE_ID varchar(max),
		@LOCATION varchar(max),
		@PROC_ID VARCHAR(12),
        --@PROCESS_ID BIGINT,
        @LAST_PROCESS_ID VARCHAR(12),
        @TODAY VARCHAR(8),
        @CURR_NO INT,
        @CURR_PROCESS_ID VARCHAR(12),
		@param_0 varchar(max),
		@param_1 varchar(max),
		@param_2 varchar(max),
		@param_4 varchar(max),
		@MESSAGE_TYPE varchar(5),
		@MESSAGE_ID	varchar(50),
		@MESSAGE varchar(MAX),
		@NOW DATETIME

	-- Insert statements for procedure here
	declare
		@rowNo int = 0,
		@l_n_process_status smallint = 0,
		@l_n_return_value smallint = 0,
		@l_v_log_mesg varchar(max)--,
		--@l_n_data_count bigint

	set @l_n_process_status = 0;

	set @FUNC_ID = 'IFilePriceFromADM' -- InterfaceFilePriceFromADM
	set @MODULE_ID = 'BMPV'
	set @LogSeq = 1

	BEGIN TRY
		
		-- INSERT LOG HEADER
		--SET @NOW = SYSDATETIME();
		--EXEC dbo.spInsertLogHeader @PROCESS_ID, @FUNC_ID, @MODULE_ID, @NOW, @NOW, 4, 'SYSTEM';
		
		print 'Masuk Begin TRY'
	
		-- 3. Mat No Existence [START]
		SET @param_0 = 'Material Master Table'
		SET @LOCATION = 'Material Master Checking'
		SET @MESSAGE_ID = 'MICS3320BINF'
		EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT, @param_0
		SET @MESSAGE_TYPE = 'INF'
					
		EXEC dbo.SPInsertLogDetail @process_id
		,@MESSAGE_ID
		,@MESSAGE_TYPE
		,@MESSAGE
		,@LOCATION
		,'SYSTEM'
		,null;

		DECLARE @mat_no VARCHAR(25)
		DECLARE l_cursor CURSOR LOCAL FAST_FORWARD
		FOR
			SELECT MAT_NO
				FROM [TB_T_DRAFT_MATERIAL_PRICE]
				GROUP BY MAT_NO
	
		OPEN l_cursor
		FETCH NEXT FROM l_cursor INTO
			@mat_no
		BEGIN
			WHILE @@FETCH_STATUS = 0
			BEGIN
				set @rowNo = @rowNo + 1
				BEGIN TRY
					print 'Masuk Begin TRY Mat No Existence With Mat No = '+@mat_no+''
					IF NOT EXISTS(SELECT MAT_NO FROM [TB_M_MATERIAL_PRICE] WHERE MAT_NO = @mat_no)
					BEGIN
						SET @param_0 = 'Row '+CONVERT(varchar(10), @rowNo)+' Material No '+@mat_no+''
						SET @param_1 = 'Material Master Table'
						SET @MESSAGE_ID = 'MICS3320BERR'
						EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT, @param_0, @param_1
						SET @MESSAGE_TYPE = 'ERR'
						SET @LOCATION = 'Data Existance'
					
						EXEC dbo.SPInsertLogDetail @process_id
						,@MESSAGE_ID
						,@MESSAGE_TYPE
						,@MESSAGE
						,@LOCATION
						,'SYSTEM'
						,null;


						-- Insert Log End Process
						SET @MESSAGE_ID = 'MPCS00123ERR'
						EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT, @FUNC_ID
						SET @MESSAGE_TYPE = 'ERR'
						SET @LOCATION = 'Material Master Checking'
					
						EXEC dbo.SPInsertLogDetail @process_id
						,@MESSAGE_ID
						,@MESSAGE_TYPE
						,@MESSAGE
						,@LOCATION
						,'SYSTEM'
						,null;

						--Update Error Log_H
						UPDATE TB_R_LOG_H SET PROCESS_STATUS = '1' WHERE PROCESS_ID = @PROCESS_ID

						-- Return To Bacth
						set @ro_v_err_mesg = 'ERROR'
						set @l_n_process_status = 1
						return @l_n_process_status
					END
				END TRY
				BEGIN CATCH
					print 'Masuk Begin Catch Mat No Existence'
					

				END CATCH

				FETCH NEXT FROM l_cursor INTO
				@mat_no
			END
		END
		CLOSE l_cursor
		DEALLOCATE l_cursor
		-- Mat No Existence [END]

		-- 4. Supplier Master existence [START]
		SET @param_0 = 'Supplier Master Table'
		SET @LOCATION = 'Supplier Master Checking'
		SET @MESSAGE_ID = 'MICS3331BINF'
		EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT, @param_0
		SET @MESSAGE_TYPE = 'INF'
					
		EXEC dbo.SPInsertLogDetail @process_id
		,@MESSAGE_ID
		,@MESSAGE_TYPE
		,@MESSAGE
		,@LOCATION
		,'SYSTEM'
		,null;

		SET @rowNo = 0
		DECLARE @supplier_code VARCHAR(25)
		DECLARE l_cursor CURSOR LOCAL FAST_FORWARD
		FOR
			SELECT SUPP_CD
				FROM [TB_T_DRAFT_MATERIAL_PRICE]
				GROUP BY SUPP_CD
	
		OPEN l_cursor
		FETCH NEXT FROM l_cursor INTO
			@supplier_code
		BEGIN
			WHILE @@FETCH_STATUS = 0
			BEGIN
				set @rowNo = @rowNo + 1
				BEGIN TRY
					print 'Masuk Begin TRY Supplier Master Existence With Supplier Code = '+@supplier_code+''
					IF NOT EXISTS(SELECT SUPPLIER_CODE FROM TB_M_SUPPLIER WHERE SUPPLIER_CODE = @supplier_code)
					BEGIN
						SET @param_0 = 'Row '+CONVERT(varchar(10), @rowNo)+' Supplier Code '+@supplier_code+''
						SET @param_1 = 'Supplier Master Table'
						SET @MESSAGE_ID = 'MICS3331BERR'
						EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT, @param_0, @param_1
						SET @MESSAGE_TYPE = 'ERR'
						SET @LOCATION = 'Data Existance'
					
						EXEC dbo.SPInsertLogDetail @process_id
						,@MESSAGE_ID
						,@MESSAGE_TYPE
						,@MESSAGE
						,@LOCATION
						,'SYSTEM'
						,null;

						-- Insert Log End Process
						SET @MESSAGE_ID = 'MPCS00123ERR'
						EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT, @FUNC_ID
						SET @MESSAGE_TYPE = 'ERR'
						SET @LOCATION = 'Supplier Master Checking'
					
						EXEC dbo.SPInsertLogDetail @process_id
						,@MESSAGE_ID
						,@MESSAGE_TYPE
						,@MESSAGE
						,@LOCATION
						,'SYSTEM'
						,null;

						--Update Error Log_H
						UPDATE TB_R_LOG_H SET PROCESS_STATUS = '1' WHERE PROCESS_ID = @PROCESS_ID

						-- Return To Bacth
						set @ro_v_err_mesg = 'ERROR'
						set @l_n_process_status = 1
						return @l_n_process_status
					END
				END TRY
				BEGIN CATCH
					print 'Masuk Begin Catch Supplier Master Existence'

				END CATCH

				FETCH NEXT FROM l_cursor INTO
				@mat_no
			END
		END
		CLOSE l_cursor
		DEALLOCATE l_cursor
		-- Supplier Master existence [END]

		-- 5. Source List existence [START]
		SET @param_0 = 'Source Type Master Tablee'
		SET @LOCATION = 'Source List Master Checking'
		SET @MESSAGE_ID = 'MICS3329BWRN'
		EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT, @param_0
		SET @MESSAGE_TYPE = 'INF'
					
		EXEC dbo.SPInsertLogDetail @process_id
		,@MESSAGE_ID
		,@MESSAGE_TYPE
		,@MESSAGE
		,@LOCATION
		,'SYSTEM'
		,null;

		SET @rowNo = 0
		DECLARE @sl_supp_cd VARCHAR(25), @sl_mat_no VARCHAR(25), @sl_prod_purpose VARCHAR(25), @sl_source_type VARCHAR(25)
		DECLARE l_cursor CURSOR LOCAL FAST_FORWARD
		FOR
			SELECT MAT_NO, SOURCE_TYPE, PROD_PURPOSE_CD, SUPP_CD
				FROM [TB_T_DRAFT_MATERIAL_PRICE]
				GROUP BY MAT_NO, SOURCE_TYPE, PROD_PURPOSE_CD, SUPP_CD
	
		OPEN l_cursor
		FETCH NEXT FROM l_cursor INTO
			@sl_mat_no, @sl_source_type, @sl_prod_purpose, @sl_supp_cd
		BEGIN
			WHILE @@FETCH_STATUS = 0
			BEGIN
				set @rowNo = @rowNo + 1
				BEGIN TRY
					print 'Masuk Begin TRY Supplier Master Existence With Mat No = '+@sl_mat_no+' ,Source Type = '+@sl_source_type+' ,Prod Purpose = '+@sl_prod_purpose+' ,Supplier Code = '+@sl_supp_cd
					IF NOT EXISTS(SELECT MAT_NO, SOURCE_TYPE, PROD_PURPOSE_CD, SUPP_CD FROM TB_M_SOURCE_LIST
								WHERE MAT_NO = @sl_mat_no
								AND SOURCE_TYPE = @sl_source_type
								AND PROD_PURPOSE_CD = @sl_prod_purpose
								AND SUPP_CD = @sl_supp_cd)
					BEGIN
						--SET @rowNo = @rowNo + 1
						SET @param_0 = 'Row '+CONVERT(varchar(10), @rowNo)+' Material No '+@sl_mat_no+', Prod Purpose '+@sl_prod_purpose+', Source Type'+@sl_source_type+', Supplier Code'+@sl_supp_cd+''
						SET @param_1 = 'Source List Material Source Table'
						SET @MESSAGE_ID = 'MICS3321BERR'
						EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT, @param_0, @param_1
						SET @MESSAGE_TYPE = 'ERR'
						SET @LOCATION = 'Check Material Master and Source Table'
					
						EXEC dbo.SPInsertLogDetail @process_id
						,@MESSAGE_ID
						,@MESSAGE_TYPE
						,@MESSAGE
						,@LOCATION
						,'SYSTEM'
						,null;

						INSERT INTO 
						TB_M_SOURCE_LIST(	
									 MAT_NO
									,PROD_PURPOSE_CD
									,SOURCE_TYPE
									,SUPP_CD
									,PART_COLOR_SFX
									,VALID_DT_FR
									,VALID_DT_TO
									,CREATED_BY
									,CREATED_DT)	
						SELECT 
									 MAT_NO
									,PROD_PURPOSE_CD
									,SOURCE_TYPE
									,SUPP_CD
									,PART_COLOR_SFX
									,VALID_DT_FR
									,VALID_DT_TO
									,CREATED_BY
									,CREATED_DT
						FROM TB_T_DRAFT_MATERIAL_PRICE

						---- Insert Log End Process
						--SET @MESSAGE_ID = 'MPCS00123ERR'
						--EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT, @FUNC_ID
						--SET @MESSAGE_TYPE = 'ERR'
						--SET @LOCATION = 'Source List Master Checking'
					
						--EXEC dbo.SPInsertLogDetail @process_id
						--,@MESSAGE_ID
						--,@MESSAGE_TYPE
						--,@MESSAGE
						--,@LOCATION
						--,'SYSTEM'
						--,null;

						-- set @l_n_process_status = 1
						--return @l_n_process_status
					END
				END TRY
				BEGIN CATCH
					print 'Masuk Begin Catch Source List Existence'

				END CATCH

				FETCH NEXT FROM l_cursor INTO
				@sl_mat_no, @sl_source_type, @sl_prod_purpose, @sl_supp_cd
			END
		END
		CLOSE l_cursor
		DEALLOCATE l_cursor
		-- Source List existence [END]

		-- 7-8 Duplication Checking [START]
		SET @LOCATION = 'Exec Duplication validation'
		SET @MESSAGE_ID = 'MICS0537BINF'
		EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT
		SET @MESSAGE_TYPE = 'INF'
					
		EXEC dbo.SPInsertLogDetail @process_id
		,@MESSAGE_ID
		,@MESSAGE_TYPE
		,@MESSAGE
		,@LOCATION
		,'SYSTEM'
		,null;

		DECLARE @dc_warp_reff_cd varchar(10), @dc_supp_cd VARCHAR(25), @dc_mat_no VARCHAR(25), @dc_source_type VARCHAR(25), @dc_valid_fr VARCHAR(25), 
			@dc_prod_purpose VARCHAR(25), @dc_part_clr_sfx VARCHAR(25), @dc_packing_type VARCHAR(25), @dc_jumlah VARCHAR(25)
		---- 7. Duplication Checking Draft Material Price[START]
			
		--	IF EXISTS(SELECT WARP_REF_NO, SUPP_CD, MAT_NO, SOURCE_TYPE, VALID_DT_FR, PROD_PURPOSE_CD, PART_COLOR_SFX, PACKING_TYPE, COUNT(*) AS JUMLAH
		--		FROM TB_T_DRAFT_MATERIAL_PRICE
		--		GROUP BY WARP_REF_NO, SUPP_CD, MAT_NO, SOURCE_TYPE, VALID_DT_FR, PROD_PURPOSE_CD, PART_COLOR_SFX, PACKING_TYPE
		--		HAVING COUNT(*) > 1)
		--	BEGIN
		--		-- Get Value Duplicate
		--		print 'Masuk Begin Duplicate Staging'
		--		SELECT TOP 1 @dc_mat_no = MAT_NO, @dc_supp_cd = SUPP_CD, @dc_prod_purpose = PROD_PURPOSE_CD, @dc_source_type = SOURCE_TYPE, @dc_valid_fr = VALID_DT_FR,
		--					 @dc_prod_purpose = PROD_PURPOSE_CD, @dc_part_clr_sfx = PART_COLOR_SFX, @dc_packing_type = PACKING_TYPE, @dc_jumlah = COUNT(*)
		--			FROM TB_T_DRAFT_MATERIAL_PRICE
		--		GROUP BY SUPP_CD, MAT_NO, SOURCE_TYPE, VALID_DT_FR, PROD_PURPOSE_CD, PART_COLOR_SFX, PACKING_TYPE
		--		HAVING COUNT(*) > 1

		--		SET @param_0 = 'Row '+CONVERT(varchar(10), @rowNo)+' ,SUPPLIER CODE '+@dc_supp_cd+' ,MATERIAL NO '+@dc_mat_no+' ,SOURCE TYPE '+@dc_source_type+' ,VALID DT FROM '+@dc_valid_fr+'
		--						,PROD PURPOSE '+@dc_prod_purpose+' ,PART COLOR SUFFIX '+@dc_part_clr_sfx+' ,PACKING TYPE '+@dc_packing_type
		--		SET @MESSAGE_ID = 'MICS0537BERR'
		--		EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT, @param_0
		--		SET @MESSAGE_TYPE = 'ERR'
		--		SET @LOCATION = 'Duplication Checking in Staging'
					
		--		EXEC dbo.SPInsertLogDetail @process_id
		--		,@MESSAGE_ID
		--		,@MESSAGE_TYPE
		--		,@MESSAGE
		--		,@LOCATION
		--		,'SYSTEM'
		--		,null;

		--		-- Insert Log End Process
		--		SET @MESSAGE_ID = 'MPCS00123ERR'
		--		EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT, @FUNC_ID
		--		SET @MESSAGE_TYPE = 'ERR'
		--		SET @LOCATION = 'Duplication Checking in Staging'
					
		--		EXEC dbo.SPInsertLogDetail @process_id
		--		,@MESSAGE_ID
		--		,@MESSAGE_TYPE
		--		,@MESSAGE
		--		,@LOCATION
		--		,'SYSTEM'
		--		,null;


		--		--Update Error Log_H
		--		UPDATE TB_R_LOG_H SET PROCESS_STATUS = '1' WHERE PROCESS_ID = @PROCESS_ID
		--		set @l_n_process_status = 1
		--		return @l_n_process_status
		--	END
		-- 7. Duplication Checking Draft Material Price[END]

		-- 8. Duplication Checking Draft Material Price[START]
			IF EXISTS(
					SELECT *
					FROM TB_T_DRAFT_MATERIAL_PRICE T JOIN TB_M_DRAFT_MATERIAL_PRICE M
						ON T.MAT_NO = M.MAT_NO 
					WHERE T.SOURCE_TYPE = M.SOURCE_TYPE
					AND T.SUPP_CD = M.SUPP_CD
					AND T.PROD_PURPOSE_CD = M.PROD_PURPOSE_CD
					AND T.PART_COLOR_SFX = M.PART_COLOR_SFX
					AND T.PACKING_TYPE = M.PACKING_TYPE
					--AND T.VALID_DT_FR = M.VALID_DT_FR
					--AND T.PRICE_AMT = M.PRICE_AMT
					)
			BEGIN
				-- Get Value Duplicate
				print 'Masuk Begin Duplicate Draft Material Price & Staging'
				DECLARE l_cursor CURSOR LOCAL FAST_FORWARD
				FOR
					SELECT T.SUPP_CD, T.MAT_NO, T.SOURCE_TYPE, T.VALID_DT_FR, T.PROD_PURPOSE_CD, T.PART_COLOR_SFX, T.PACKING_TYPE
					FROM TB_T_DRAFT_MATERIAL_PRICE T JOIN TB_M_DRAFT_MATERIAL_PRICE M
						ON T.MAT_NO = M.MAT_NO 
					WHERE T.SOURCE_TYPE = M.SOURCE_TYPE
					AND T.SUPP_CD = M.SUPP_CD
					AND T.PROD_PURPOSE_CD = M.PROD_PURPOSE_CD
					AND T.PART_COLOR_SFX = M.PART_COLOR_SFX
					AND T.PACKING_TYPE = M.PACKING_TYPE
	
				OPEN l_cursor
				FETCH NEXT FROM l_cursor INTO
					@dc_supp_cd, @dc_mat_no, @dc_source_type, @dc_valid_fr, @dc_prod_purpose, @dc_part_clr_sfx, @dc_packing_type
				BEGIN
					WHILE @@FETCH_STATUS = 0
					BEGIN
						set @rowNo = @rowNo + 1
						BEGIN TRY
							IF EXISTS(SELECT *
										FROM TB_T_DRAFT_MATERIAL_PRICE T JOIN TB_M_DRAFT_MATERIAL_PRICE M
											ON T.MAT_NO = M.MAT_NO 
										WHERE T.SOURCE_TYPE = M.SOURCE_TYPE
										AND T.SUPP_CD = M.SUPP_CD
										AND T.PROD_PURPOSE_CD = M.PROD_PURPOSE_CD
										AND T.PART_COLOR_SFX = M.PART_COLOR_SFX
										AND T.PACKING_TYPE = M.PACKING_TYPE
									)
							BEGIN
								SET @param_0 = 'Row '+CONVERT(varchar(10), @rowNo)+' ,Supplier Code '+@dc_supp_cd+' ,Material No '+@dc_mat_no+' ,Source Type '+@dc_source_type+' ,Valid Date From '+@dc_valid_fr+' ,Prod Purpose '+@dc_prod_purpose+' ,Part Color Suffix '+@dc_part_clr_sfx+' ,Packing Type '+@dc_packing_type
								SET @MESSAGE_ID = 'MICS0537BERR'
								EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT, @param_0
								SET @MESSAGE_TYPE = 'ERR'
								SET @LOCATION = 'Duplication Checking in Draft Material Price Master & Staging'
					
								EXEC dbo.SPInsertLogDetail @process_id
								,@MESSAGE_ID
								,@MESSAGE_TYPE
								,@MESSAGE
								,@LOCATION
								,'SYSTEM'
								,null;


								-- Insert Log End Process
								SET @MESSAGE_ID = 'MPCS00123ERR'
								EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT, @FUNC_ID
								SET @MESSAGE_TYPE = 'ERR'
								SET @LOCATION = 'Duplication Checking in Draft Material Price Master & Staging'
					
								EXEC dbo.SPInsertLogDetail @process_id
								,@MESSAGE_ID
								,@MESSAGE_TYPE
								,@MESSAGE
								,@LOCATION
								,'SYSTEM'
								,null;

								--Update Error Log_H
								UPDATE TB_R_LOG_H SET PROCESS_STATUS = '1' WHERE PROCESS_ID = @PROCESS_ID

								-- Return To Bacth
								set @ro_v_err_mesg = 'ERROR'
								set @l_n_process_status = 1
								return @l_n_process_status
							END
						END TRY
						BEGIN CATCH
							print 'Masuk Begin Catch Mat No Existence'
					

						END CATCH

						FETCH NEXT FROM l_cursor INTO
						@dc_supp_cd, @dc_mat_no, @dc_source_type, @dc_valid_fr, @dc_prod_purpose, @dc_part_clr_sfx, @dc_packing_type
					END
				END
				CLOSE l_cursor
				DEALLOCATE l_cursor

				
			END
		-- 8. Duplication Checking Draft Material Price[END]
		-- Duplication Checking[END]

		-- 9. Check New CPP[START]
		SET @LOCATION = 'Exec Check CPP'
		SET @MESSAGE_ID = 'MICS0430BINF'
		EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT
		SET @MESSAGE_TYPE = 'INF'
					
		EXEC dbo.SPInsertLogDetail @process_id
		,@MESSAGE_ID
		,@MESSAGE_TYPE
		,@MESSAGE
		,@LOCATION
		,'SYSTEM'
		,null;


		-- Get Value Duplicate
		print 'Masuk Begin Check CPP Flag'
			
		SET @rowNo = 0
		DECLARE @cpp_warp_reff_cd varchar(10), @cpp_supp_cd VARCHAR(25), @cpp_mat_no VARCHAR(25), @cpp_valid_fr VARCHAR(25), 
			@cpp_price_sts VARCHAR(25), @cpp_part_clr_sfx VARCHAR(25), @cpp_flag VARCHAR(25)
		DECLARE l_cursor CURSOR LOCAL FAST_FORWARD
		FOR
			SELECT WARP_REF_NO, SUPP_CD, MAT_NO, VALID_DT_FR, PART_COLOR_SFX, PRICE_STATUS, CPP_FLAG
				FROM [TB_T_DRAFT_MATERIAL_PRICE]
				GROUP BY WARP_REF_NO, SUPP_CD, MAT_NO, VALID_DT_FR, PART_COLOR_SFX, PRICE_STATUS, CPP_FLAG
	
		OPEN l_cursor
		FETCH NEXT FROM l_cursor INTO
			@cpp_warp_reff_cd, @cpp_supp_cd, @cpp_mat_no, @cpp_valid_fr, @cpp_part_clr_sfx, @cpp_price_sts, @cpp_flag
		BEGIN
			WHILE @@FETCH_STATUS = 0
			BEGIN
				set @rowNo = @rowNo + 1
				BEGIN TRY
					--print 'Masuk Begin TRY Check New CPP With Warp Reff No = '+@cpp_warp_reff_cd+' ,Supplier Code = '+@cpp_supp_cd+' ,Material No = '+@cpp_mat_no+' ,Valid Date From = '+@cpp_valid_fr+', Part Color Sufix = '+@cpp_part_clr_sfx+', Price Status = '+@cpp_price_sts+', CPP Flag = '+@cpp_flag
					IF EXISTS(SELECT MAT_NO, PART_COLOR_SFX, SUPP_CD, VALID_DT_FR, PRICE_STATUS, CPP_FLAG
									FROM TB_T_DRAFT_MATERIAL_PRICE
								WHERE CPP_FLAG = 'Y'
								GROUP BY MAT_NO, PART_COLOR_SFX, SUPP_CD, VALID_DT_FR, PRICE_STATUS, CPP_FLAG)
					BEGIN
						SET @param_0 = 'Row '+CONVERT(varchar(10), @rowNo)+' Warp Reff No '+@cpp_warp_reff_cd+' ,Supplier Code '+@cpp_supp_cd+' ,Material No '+@cpp_mat_no+' ,Valid Date From '+@cpp_valid_fr+', Part Color Sufix '+@cpp_part_clr_sfx+', Price Status '+@cpp_price_sts+', CPP Flag = YES'
						SET @MESSAGE_ID = 'MICS0429BINF'
						EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT, @param_0
						SET @MESSAGE_TYPE = 'INF'
						SET @LOCATION = 'Check new CPP'
					
						EXEC dbo.SPInsertLogDetail @process_id
						,@MESSAGE_ID
						,@MESSAGE_TYPE
						,@MESSAGE
						,@LOCATION
						,'SYSTEM'
						,null;
					END
				END TRY
				BEGIN CATCH
					print 'Masuk Begin Catch Check CPP Flag'

				END CATCH

				FETCH NEXT FROM l_cursor INTO
				@cpp_warp_reff_cd, @cpp_supp_cd, @cpp_mat_no, @cpp_valid_fr, @cpp_part_clr_sfx, @cpp_price_sts, @cpp_flag
			END
		END
		CLOSE l_cursor
		DEALLOCATE l_cursor

		-- 9. Check New CPP[END]

		-- 10. Insert draft material price data [START]
		SET @LOCATION = 'Insert Data to Destination Table'
		SET @MESSAGE_ID = 'MIPPCS040INF'
		EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT
		SET @MESSAGE_TYPE = 'INF'
					
		EXEC dbo.SPInsertLogDetail @process_id
		,@MESSAGE_ID
		,@MESSAGE_TYPE
		,@MESSAGE
		,@LOCATION
		,'SYSTEM'
		,null;

		-- Insert into table TB_M_DRAFT_MATERIAL_PRICE
		INSERT INTO 
		TB_M_DRAFT_MATERIAL_PRICE(
					WARP_BUYER_CD
					,SOURCE_DATA
					,DRAFT_DF
					,WARP_REF_NO
					,CPP_FLAG
					,MAT_NO
					,PROD_PURPOSE_CD
					,SOURCE_TYPE
					,SUPP_CD
					,PART_COLOR_SFX
					,PACKING_TYPE
					,PRICE_AMT
					,CURR_CD
					,VALID_DT_FR
					,VALID_DT_TO
					,CREATED_BY
					,CREATED_DT)
		SELECT
					WARP_BUYER_CD
					,SOURCE_DATA
					,DRAFT_DF
					,WARP_REF_NO
					,CPP_FLAG
					,MAT_NO
					,PROD_PURPOSE_CD
					,SOURCE_TYPE
					,SUPP_CD
					,PART_COLOR_SFX
					,PACKING_TYPE
					,PRICE_AMT
					,CURR_CD
					,VALID_DT_FR
					,VALID_DT_TO
					,CREATED_BY
					,CREATED_DT
		FROM
		TB_T_DRAFT_MATERIAL_PRICE

		----Insert into table TB_M_DRAFT_MATERIAL_PRICE
		--INSERT INTO 
		--TB_M_SOURCE_LIST(	
		--			 MAT_NO
		--			,PROD_PURPOSE_CD
		--			,SOURCE_TYPE
		--			,SUPP_CD
		--			,PART_COLOR_SFX
		--			,VALID_DT_FR
		--			,VALID_DT_TO)	
		--SELECT 
		--			 MAT_NO
		--			,PROD_PURPOSE_CD
		--			,SOURCE_TYPE
		--			,SUPP_CD
		--			,PART_COLOR_SFX
		--			,VALID_DT_FR
		--			,VALID_DT_TO
		--FROM TB_M_SOURCE_LIST

		-- 10. Insert draft material price data [END]


		-- INSERT LOG HEADER TO UPDATE STATUS
		UPDATE TB_R_LOG_H SET PROCESS_STATUS = '0' WHERE PROCESS_ID = @PROCESS_ID

		SET @LOCATION = 'End Proccess'
		SET @MESSAGE_ID = 'MIPPCS050INF'
		EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT
		SET @MESSAGE_TYPE = 'INF'
					
		EXEC dbo.SPInsertLogDetail @process_id
		,@MESSAGE_ID
		,@MESSAGE_TYPE
		,@MESSAGE
		,@LOCATION
		,'SYSTEM'
		,null;

		-- Message
		set @ro_v_err_mesg = 'SUCCESS'
	END TRY
	BEGIN CATCH
		print 'Masuk Begin Catch Terakhir'
		SET @LOCATION = 'End Proccess'
		SET @MESSAGE_ID = 'MIPPCS050ERR'
		EXEC SP_GET_MESSAGE @MESSAGE_ID, @MESSAGE OUTPUT, @N_ERR OUTPUT
		SET @MESSAGE_TYPE = 'ERR'
					
		EXEC dbo.SPInsertLogDetail @process_id
		,@MESSAGE_ID
		,@MESSAGE_TYPE
		,@MESSAGE
		,@LOCATION
		,'SYSTEM'
		,null;


		declare @ErrorMessage nvarchar(4000),
				@ErrorSaverity int,
				@ErrorState int,
				@ErrorLine int

		select @ErrorMessage = ERROR_MESSAGE(),
				@ErrorSaverity = ERROR_SEVERITY(),
				@ErrorState = ERROR_STATE(),
				@ErrorLine = ERROR_LINE()

		set @l_n_process_status = 2
		set @ro_v_err_mesg = 'ERROR: SP_InterfaceFilePriceFromADM: ' +@ErrorMessage+ ', at line = ' + CAST (@ErrorLine as varchar)

		print 'SP_InterfaceFilePriceFromADM: ' +@ErrorMessage+ ', at line = ' + cast (@ErrorLine as varchar)

		--Update Error Log_H
		UPDATE TB_R_LOG_H SET PROCESS_STATUS = '2' WHERE PROCESS_ID = @PROCESS_ID

		return @l_n_process_status

	END CATCH
END
GO



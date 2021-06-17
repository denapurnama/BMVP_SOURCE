CREATE PROCEDURE [dbo].[SP_SEND_GR_BMPV_TO_ADM_BATCH_CREATE_FILE_CTF] 
	@ri_n_process_id		bigint,
	@ri_v_file_id			varchar(100),
	@ri_v_interface_name	varchar(100),
	@ri_v_module_id			varchar(20),
	@ri_v_function_id		varchar(20),
	@ri_v_source_files		varchar(100),
	@ri_v_user_id			varchar(20),
	@ri_n_err				int OUTPUT
AS
BEGIN
  DECLARE 
		@l_v_user_name			varchar(20) = 'system',
        @l_v_location			varchar(100) = '42004',
		@l_v_fqdn				varchar(8000) = '',
		@l_v_log_position		varchar(max) = 'Create File GR BMPV .CTF',
		@l_v_business_err_key	varchar(20) = '##ERROR_BUSINESS##',
		@l_v_process_status     varchar(1) = '0',
        @l_v_fgw_ip				varchar(32),
        @l_v_fgw_user			varchar(20),
        @l_v_fgw_pass			varchar(100),
        @l_v_cmd2				varchar(max),
		@l_v_file_size			varchar(100),
		@l_v_msg_text			varchar(max),
		@l_v_path_temp			varchar(MAX),
        @l_v_cmd				varchar(8000),
        @l_v_sql_queries		varchar(max),
        @l_v_bcp_user			varchar(20),
		@l_v_bcp_pass			varchar(MAX),
		@l_v_bcp_instance		varchar(100),
		@l_v_bcp_db				varchar(100), 
        @l_v_source_files_ctf	varchar(100),
		@l_v_log_mesg_id		varchar(12),
		@l_v_log_mesg			varchar(max),
        @l_c_host_type			char(1),
		@l_c_transfer_type		char(1), 
		@l_c_send_type			char(1),
		@l_c_comp_mode			char(1),
		@l_c_reg_mode			char(1),
		@l_c_sync_async			char(1),
		@l_c_gnco				char(1),
		@l_n_return_value		tinyint = 0,
		@l_n_process_status     tinyint = 0,
		@l_b_have_warning_log	bit = 0
		
	CREATE TABLE #createfilectf (
		id int IDENTITY (1, 1),
		s varchar(1000)
	)
  
	SELECT
		@l_v_path_temp = path_create_db,
		@l_v_bcp_user = UserName_Db_server,
		@l_v_bcp_pass = password_Db_server,
		@l_v_bcp_instance = Db_server,
		@l_v_bcp_db = [DB_NAME]
	FROM TB_M_INTERFACE_FILE
	WHERE  INTERFACE_NAME = @ri_v_interface_name

	SELECT @l_v_fgw_ip = SYSTEM_VALUE FROM TB_M_SYSTEM WHERE FUNCTION_ID = '42003' AND SYSTEM_CD = 'IP_ADDRESS'
	SELECT @l_v_fgw_user = SYSTEM_VALUE FROM TB_M_SYSTEM WHERE FUNCTION_ID = '42003' AND SYSTEM_CD = 'USER_ID'
	SELECT @l_v_fgw_pass = SYSTEM_VALUE FROM TB_M_SYSTEM WHERE FUNCTION_ID = '42003' AND SYSTEM_CD = 'PASSWORD'
	SELECT @l_c_host_type = SYSTEM_VALUE FROM TB_M_SYSTEM WHERE FUNCTION_ID = '42003' AND SYSTEM_CD = 'HOST_TYPE'
	SELECT @l_c_send_type = SYSTEM_VALUE FROM TB_M_SYSTEM WHERE FUNCTION_ID = '42003' AND SYSTEM_CD = 'SEND_TYPE'
	SELECT @l_c_transfer_type = SYSTEM_VALUE FROM TB_M_SYSTEM WHERE FUNCTION_ID = '42003' AND SYSTEM_CD = 'TRANSFER_TYPE'
	SELECT @l_c_comp_mode = SYSTEM_VALUE FROM TB_M_SYSTEM WHERE FUNCTION_ID = '42003' AND SYSTEM_CD = 'COMPRESSION_MODE'
	SELECT @l_c_reg_mode = SYSTEM_VALUE FROM TB_M_SYSTEM WHERE FUNCTION_ID = '42003' AND SYSTEM_CD = 'REGISTRATION_MODE'
	SELECT @l_c_sync_async = SYSTEM_VALUE FROM TB_M_SYSTEM WHERE FUNCTION_ID = '42003' AND SYSTEM_CD = 'SYNC_ASYNC'
	SELECT @l_c_gnco = SYSTEM_VALUE FROM TB_M_SYSTEM WHERE FUNCTION_ID = '42003' AND SYSTEM_CD = 'GENERATION_CONTROL_NO'

	SET @l_v_source_files_ctf = @ri_v_source_files + '.CTF'
	EXEC [SP_SEND_GR_TO_ADM_BATCH_GET_FILE_SIZE] @l_v_path_temp, @ri_v_source_files, @l_v_file_size OUT
	
	SET @l_v_sql_queries = 
		'"SELECT CHAR(0) + ''' + @l_v_fgw_ip + 
			''' + CHAR(0) + ''' + @l_v_fqdn + 
			''' + CHAR(0) + ''' + @l_c_host_type + 
			''' + CHAR(0) + ''' + @l_c_send_type + 
			''' + CHAR(0) + ''' + @l_c_transfer_type + 
			''' + CHAR(0) + ''' + @l_c_comp_mode + 
			''' + CHAR(0) + ''' + @l_v_fgw_user + 
			''' + CHAR(0) + ''' + @l_v_fgw_pass + 
			''' + CHAR(0) + CHAR(0) + CHAR(0) + CHAR(0) + CHAR(0) + CHAR(0) + CHAR(0) + ''' + @l_c_reg_mode + 
			''' + CHAR(0) +  ''' + REPLACE(@l_v_file_size, ',', '') + 
			''' + CHAR(0) + CHAR(0) + ''' + @l_c_sync_async + 
			''' + CHAR(0) + ''' + @l_c_gnco + 
			''' + CHAR(10) + CHAR(13)"'

	EXEC SP_PUTLOG @l_v_sql_queries, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR000INF', 'INF', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;
	
	--SELECT @sql
	--SELECT @FGW_IP, @FQDN, @HOSTTYPE, @SENDTYPE, @TRANSFERTYPE, @COMPMODE, @FGWUser, @FGWPassword, @REGMODE, @FILE_SIZE, @SYNCASYNC, @GCNO

	-- SELECT
		-- @sql [sql],
		-- @FGW_IP FGW_IP,
		-- @FQDN FQDN,
		-- @HOSTTYPE HOSTTYPE,
		-- @SENDTYPE SENDTYPE,
		-- @TRANSFERTYPE TRANSFERTYPE,
		-- @COMPMODE COMPMODE,
		-- @FTPUSERID FTPUSERID,
		-- @FTPUSERPASS FTPUSERPASS,
		-- @REGMODE REGMODE,
		-- @FILE_SIZE FILE_SIZE,
		-- @SYNCASYNC SYNCASYNC,
		-- @GCNO GCNO
	
	SET @l_v_cmd = 'bcp ' + @l_v_sql_queries + ' queryout "' + @l_v_path_temp + @l_v_source_files_ctf + '" -U ' + @l_v_bcp_user + ' -P ' + @l_v_bcp_pass + ' -S ' + @l_v_bcp_instance + ' -d ' + @l_v_bcp_db + ' -r -c -t'
	--set @cmd2='bcp '  + '"select ''char(13)''" queryout  "'+@path_temp+@SourceFilesCTF+' " -U '+@BCPUSER+' -P '+@BCPPASS+' -S '+@BCPINSTANCE+' -d '+@BCPDB+' -r -c -t'
	INSERT #createfilectf EXEC master..XP_CMDSHELL @l_v_cmd

	--SELECT @cmd

	SET @ri_n_err = ISNULL((SELECT
							COUNT('')
							FROM #createfilectf
							WHERE s LIKE '%cannot find%'
							OR s LIKE '%invalid%'
							OR s LIKE '%failed%'
							OR s LIKE '%No such process%'
							OR s LIKE '%not found%'
							OR s LIKE '%Unable to open%'
							OR s LIKE '%incorrect%')
						, 0)

	IF (@ri_n_err > 0 OR (SELECT COUNT('') FROM #createfilectf) = 1)
	BEGIN
		SELECT @l_v_msg_text = s
		FROM #createfilectf
		WHERE s LIKE '%cannot find%'
			 OR s LIKE '%invalid%'
			 OR s LIKE '%failed%'
			 OR s LIKE '%No such process%'
			 OR s LIKE '%not found%'
			 OR s LIKE '%Unable to open%'
			 OR s LIKE '%incorrect%'
		
		EXEC SP_GET_MESSAGE 'MSPXPR009ERR', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @l_v_msg_text
		EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR009ERR', 'ERR', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;
		SET @ri_n_err = ISNULL(@ri_n_err, 0) + 1
	END
	ELSE
	BEGIN
		EXEC SP_GET_MESSAGE 'MSPXPR010INF', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @l_v_source_files_ctf
		EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR010INF', 'INF', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;
	END

	DROP TABLE #createfilectf
END

GO



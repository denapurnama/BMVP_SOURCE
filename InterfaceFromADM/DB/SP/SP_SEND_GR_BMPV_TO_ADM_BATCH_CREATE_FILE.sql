CREATE PROCEDURE [dbo].[SP_SEND_GR_BMPV_TO_ADM_BATCH_CREATE_FILE]
	@ri_n_process_id		bigint,
	@ri_v_file_id			varchar(100),
	@ri_v_interface_name	varchar(100),
	@ri_v_module_id			varchar(20),
	@ri_v_function_id		varchar(20),
	@ri_v_user_id			varchar(20),
	@ri_v_sql_queries		varchar(MAX),
	@ri_n_err				int OUTPUT,
	@ri_v_file_date			varchar(max) = ''
AS
BEGIN
	DECLARE 
		@l_v_log_position		varchar(max) = 'Interface File Generator Function',
		@l_v_log_mesg_id		varchar(12) = '',
		@l_v_log_mesg			varchar(max) = '',
		@l_v_business_err_key	varchar(20) = '##ERROR_BUSINESS##',
		@l_v_process_status     varchar(1) = '0',
		@l_v_location			varchar(100) = 'generate_interface_file',
		@l_v_username			varchar(20) = 'system',
		@l_v_cmd				varchar(8000),
		@l_v_cmd2				varchar(8000),
		@l_v_path_temp			varchar(MAX),
		@l_v_bcp_user			varchar(20),
		@l_v_bcp_pass			varchar(MAX),
		@l_v_bcp_instance		varchar(100),
		@l_v_bcp_db				varchar(100), 
		@l_v_send_path_db		varchar(100),
		@l_v_source_files		varchar(100),
		@l_v_source_files_ctf	varchar(100),
		@l_v_ftp_ip_address		varchar(32),
		@l_v_ftp_user			varchar(40),
		@l_v_ftp_pass			varchar(20),
		@l_v_error_path			varchar(max),
		@l_v_succes_path		varchar(max),
		@l_v_msg_text			varchar(max),
		@l_v_ftp_path_send		varchar(max),
		@l_v_ftp_path_create	varchar(max),
		@l_c_ftp_host_type		char(1), 
		@l_c_ftp_transfer_type	char(1), 
		@l_c_ftp_send_type		char(1),
		@l_c_comp_mode			char(1),
		@l_c_reg_mode			char(1),
		@l_c_sync_async			char(1),
		@l_c_gnco				char(1),
		@l_n_err				int,
		@l_n_error_state		int,
		@l_n_return_value		tinyint = 0,
		@l_n_process_status     tinyint = 0,
		@l_b_have_warning_log	bit = 0
		
	SET @ri_n_err = ISNULL(@ri_n_err, 0)
	CREATE TABLE #createfile (id INT IDENTITY(1,1), s VARCHAR(1000))

	SELECT 
		@l_v_path_temp = path_create_db,
		@l_v_bcp_user = UserName_Db_server,
		@l_v_bcp_pass = password_Db_server,
		@l_v_bcp_instance = Db_server,
		@l_v_bcp_db = [DB_NAME],
		@l_v_send_path_db = path_send_db,
		@l_v_source_files = [FILE_ID],
		@l_v_ftp_path_send = path_send_FTP,
		@l_v_ftp_path_create = path_create_FTP,
		@l_v_ftp_ip_address = Ftp,
		@l_v_ftp_user = UserName_ftp,
		@l_v_ftp_pass = password_ftp,
		@l_v_error_path = path_error_db,
		@l_v_succes_path = path_succes_db
	FROM  tb_m_interface_file 
	WHERE [FILE_ID] = @ri_v_file_id 
		  AND INTERFACE_NAME = @ri_v_interface_name

	SET @l_v_source_files = @l_v_source_files + '' + @ri_v_file_date

	EXEC SP_GET_MESSAGE 'MSPXPR009INF', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT
	EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR009INF', 'INF', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;
	  
	BEGIN TRY
		SET @l_v_cmd = 'bcp ' + @ri_v_sql_queries + ' queryout "' + @l_v_path_temp + @l_v_source_files + '" -U ' + @l_v_bcp_user + ' -P ' + @l_v_bcp_pass + ' -S ' + @l_v_bcp_instance + ' -d ' + @l_v_bcp_db + ' -c'
		--SELECT @cmd cmdbpc
		SET @l_v_cmd2 = 'bcp ' + @ri_v_sql_queries + ' queryout "' + @l_v_path_temp + @l_v_source_files + '" -U '
		EXEC SP_PUTLOG @l_v_cmd2, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR000INF', 'INF', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;

		INSERT #createfile
		EXEC master..XP_CMDSHELL @l_v_cmd 

		SET @ri_n_err = ISNULL((SELECT COUNT('') FROM #createfile a JOIN TB_M_ERROR_INTERFACE_FILE b ON a.s LIKE '%' + b.Patern + '%'), 0)	

		IF @ri_n_err > 0
		BEGIN
			SELECT @l_v_msg_text = s 
			FROM #createfile 
			WHERE s LIKE '%cannot find%' 
				  OR s LIKE '%invalid%' 
				  OR s LIKE '%failed%' 
				  OR s LIKE '%No such process%' 
				  OR s LIKE '%not found%' 
				  OR s LIKE '%Unable to open%' 
				  
			EXEC SP_GET_MESSAGE 'MSPXPR009ERR', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @l_v_msg_text
			EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR009ERR', 'ERR', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;
			SET @l_n_error_state = 1
			SET @ri_n_err = ISNULL(@ri_n_err, 0) + 1
		END
		ELSE
		BEGIN
			EXEC SP_GET_MESSAGE 'MSPXPR010INF', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @l_v_source_files
			EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR010INF', 'INF', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;
		END

		DROP TABLE #createfile
		IF @ri_n_err = 0
		BEGIN
			SELECT @ri_n_err [CTF]
			EXEC [SP_SEND_GR_BMPV_TO_ADM_BATCH_CREATE_FILE_CTF] @ri_n_process_id, @ri_v_file_id, @ri_v_interface_name, @ri_v_module_id, @ri_v_function_id, @l_v_source_files, @ri_v_user_id, @ri_n_err OUTPUT
			SET @l_n_error_state = 2
		END

		SET @l_v_source_files_ctf = @l_v_source_files + '.CTF'
		
		--waitfor delay '00:00:02'
		IF @ri_n_err = 0
		BEGIN
			SELECT @ri_n_err [MOVE]
			SET @l_n_error_state = 3
			EXEC [SP_SEND_GR_BMPV_TO_ADM_BATCH_MOVE_FILE_DB] @ri_n_process_id, @ri_v_module_id, @ri_v_function_id, @l_v_path_temp, @l_v_send_path_db, @l_v_source_files, @ri_v_user_id, @ri_n_err out
		END

		IF @ri_n_err = 0
		BEGIN
			SELECT @ri_n_err [MOVE_CTF]
			SET @l_n_error_state = 4
			EXEC [SP_SEND_GR_BMPV_TO_ADM_BATCH_MOVE_FILE_DB] @ri_n_process_id, @ri_v_module_id, @ri_v_function_id, @l_v_path_temp, @l_v_send_path_db, @l_v_source_files_ctf, @ri_v_user_id, @ri_n_err out
		END
		
		--set @cmd='move ' + @path_temp + @SourceFiles + ' ' + @send_path_db + @SourceFiles
		--EXEC master..XP_CMDSHELL @cmd  

		--waitfor delay '00:00:02'
		IF @ri_n_err = 0
		BEGIN
			SELECT @ri_n_err [FTP]
			SET @l_n_error_state = 5
			EXEC [SP_SEND_GR_BMPV_TO_ADM_BATCH_UPLOAD_FTP] @ri_n_process_id, @ri_v_module_id, @ri_v_function_id, @l_v_source_files, @ri_v_file_id, @ri_v_user_id, @ri_v_interface_name, @ri_n_err OUTPUT
		END
		
		IF @ri_n_err = 0
		BEGIN
			SELECT @ri_n_err [FTP_CTF]
			SET @l_n_error_state = 6
			EXEC [SP_SEND_GR_BMPV_TO_ADM_BATCH_UPLOAD_FTP] @ri_n_process_id, @ri_v_module_id, @ri_v_function_id, @l_v_source_files_ctf, @ri_v_file_id, @ri_v_user_id, @ri_v_interface_name, @ri_n_err OUTPUT
		END

		IF @ri_n_err = 0
		BEGIN
			SET @l_n_error_state = 9
			EXEC [SP_SEND_GR_BMPV_TO_ADM_BATCH_MOVE_FILE_DB] @ri_n_process_id, @ri_v_module_id, @ri_v_function_id, @l_v_send_path_db, @l_v_succes_path, @l_v_source_files, @ri_v_user_id, @ri_n_err out, 1
		END
		
		IF @ri_n_err = 0
		BEGIN
			SET @l_n_error_state = 10
			EXEC [SP_SEND_GR_BMPV_TO_ADM_BATCH_MOVE_FILE_DB] @ri_n_process_id, @ri_v_module_id, @ri_v_function_id, @l_v_send_path_db, @l_v_succes_path, @l_v_source_files_ctf, @ri_v_user_id, @ri_n_err out, 1
		END
		
		IF @ri_n_err > 0 and @l_n_error_state in (3, 4)
		BEGIN
			EXEC [SP_SEND_GR_BMPV_TO_ADM_BATCH_MOVE_FILE_DB] @ri_n_process_id, @ri_v_module_id, @ri_v_function_id, @l_v_path_temp, @l_v_error_path, @l_v_source_files, @ri_v_user_id, @l_n_err out, 1
			EXEC [SP_SEND_GR_BMPV_TO_ADM_BATCH_MOVE_FILE_DB] @ri_n_process_id, @ri_v_module_id, @ri_v_function_id, @l_v_path_temp, @l_v_error_path, @l_v_source_files_ctf, @ri_v_user_id, @l_n_err out, 1
		END
		
		IF @ri_n_err > 0 and @l_n_error_state >= 5
		BEGIN
			EXEC [SP_SEND_GR_BMPV_TO_ADM_BATCH_MOVE_FILE_DB] @ri_n_process_id, @ri_v_module_id, @ri_v_function_id, @l_v_send_path_db, @l_v_error_path, @l_v_source_files, @ri_v_user_id, @l_n_err out, 1
			EXEC [SP_SEND_GR_BMPV_TO_ADM_BATCH_MOVE_FILE_DB] @ri_n_process_id, @ri_v_module_id, @ri_v_function_id, @l_v_send_path_db, @l_v_error_path, @l_v_source_files_ctf, @ri_v_user_id, @l_n_err out, 1
		END

		IF @ri_n_err = 0
		BEGIN
			EXEC SP_GET_MESSAGE 'MSPXPR011INF', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @l_v_source_files
			EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR011INF', 'INF', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;
		END
		ELSE
		BEGIN
			EXEC SP_GET_MESSAGE 'MSPXPR010ERR', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @l_v_source_files
			EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR010ERR', 'ERR', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;
			SET @ri_n_err = ISNULL(@ri_n_err, 0) + 1
		END
	END TRY
	BEGIN CATCH
		SET @l_n_err = ISNULL(@l_n_err, 0) + 1
		SET @l_v_log_mesg = ERROR_MESSAGE()

		RAISERROR(@l_v_log_mesg, 16, 1)
	END CATCH
END

GO



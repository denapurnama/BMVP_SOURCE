CREATE PROCEDURE [dbo].[SP_SEND_GR_BMPV_TO_ADM_BATCH_UPLOAD_FTP]
	@ri_n_process_id		bigint,
	@ri_v_module_id			varchar(20),
	@ri_v_function_id		varchar(20),
	@ri_v_file_name			varchar(128), 
	@ri_v_file_id			varchar(100), 
	@ri_v_user_id			varchar(20),
	@ri_v_interface_name	varchar(max),
	@ri_n_err				int OUTPUT,
	@ri_v_flag_msg			varchar(1) = 'Y', 
	@ri_v_move_ftp_flag		varchar(1) = 'N'
AS
BEGIN
  DECLARE 
		@l_v_user_name			varchar(20) = 'system',
		@l_v_location			varchar(100) = 'upload interface file to ftp BMPV',
		@l_v_log_position		varchar(max) = 'FTP Process',
		@l_v_business_err_key	varchar(20) = '##ERROR_BUSINESS##',
		@l_v_process_status     varchar(1) = '0',
		@l_v_work_file_name		varchar(128) = 'ftpcmd.txt',
		@l_v_cmd				varchar(1000),
		@l_v_ftp_ip_address		varchar(32),
		@l_v_ftp_user			varchar(40),
		@l_v_ftp_pass			varchar(20),
        @l_v_ftp_path			varchar(128),
        @l_v_source_path		varchar(128),
        @l_v_source_file		varchar(128),
        @l_v_workdir			varchar(128),
		@l_v_msg_text			varchar(max),
		@l_v_log_mesg_id		varchar(12),
		@l_v_log_mesg			varchar(max),
        @l_n_err				int,
		@l_n_id					int = 1,
		@l_n_return_value		tinyint = 0,
		@l_n_process_status     tinyint = 0,
		@l_b_have_warning_log	bit = 0

	CREATE TABLE #tempvartable (
		info varchar(1000)
	)

	CREATE TABLE #a (
		id int IDENTITY (1, 1),
		s varchar(1000)
	)
		
    SELECT
      @l_v_ftp_user = UserName_ftp,
      @l_v_ftp_pass = password_ftp,
      @l_v_ftp_ip_address = Ftp,
      @l_v_source_path = path_send_db,
      @l_v_ftp_path = path_create_FTP
    FROM tb_m_interface_file
    WHERE FILE_ID = @ri_v_file_id
		  AND INTERFACE_NAME = @ri_v_interface_name
  
	INSERT #tempvartable EXEC master..xp_cmdshell 'echo %temp%'
  
	SET @l_v_workdir = (SELECT TOP 1 info FROM #tempvartable)
  
	IF RIGHT(@l_v_workdir, 1) <> '\\'
	BEGIN
	SET @l_v_workdir = @l_v_workdir + '\\'
	END

	IF RIGHT(@l_v_source_path, 1) <> '\\'
	BEGIN
	SET @l_v_source_path = @l_v_source_path + '\\'
	END
	
	DROP TABLE #tempvartable

	--+ ' >> ' + @workdir + @workfilename
	--as
	/*
	exec s_ftp_PutFile 	
  		@FTPServer = 'myftpsite' ,
  		@FTPUser = 'username' ,
  		@FTPPWD = 'password' ,
  		@FTPPath = '/dir1/' ,
  		@FTPFileName = 'test2.txt' ,
  		@SourcePath = 'c:\vss\mywebsite\' ,
  		@SourceFile = 'MyFileName.html' ,
  		
  		@workdir = 'c:\temp\'
	*/

  -- deal with special characters for echo commands
  SELECT @l_v_ftp_ip_address = REPLACE(REPLACE(REPLACE(@l_v_ftp_ip_address, '|', '^|'), '<', '^<'), '>', '^>')
  SELECT @l_v_ftp_user = REPLACE(REPLACE(REPLACE(@l_v_ftp_user, '|', '^|'), '<', '^<'), '>', '^>')
  SELECT @l_v_ftp_pass = REPLACE(REPLACE(REPLACE(@l_v_ftp_pass, '|', '^|'), '<', '^<'), '>', '^>')
  SELECT @l_v_ftp_path = REPLACE(REPLACE(REPLACE(@l_v_ftp_path, '|', '^|'), '<', '^<'), '>', '^>')

  SELECT @l_v_cmd = 'echo ' + 'open ' + @l_v_ftp_ip_address + ' > ' + @l_v_workdir + @l_v_work_file_name
  EXEC master..xp_cmdshell @l_v_cmd
  
  SELECT @l_v_cmd = 'echo ' + @l_v_ftp_user + '>> ' + @l_v_workdir + @l_v_work_file_name
  EXEC master..xp_cmdshell @l_v_cmd
  
  SELECT @l_v_cmd = 'echo ' + @l_v_ftp_pass + '>> ' + @l_v_workdir + @l_v_work_file_name
  EXEC master..xp_cmdshell @l_v_cmd

  IF (ISNULL(@l_v_ftp_path, '') <> '')
  BEGIN
    SELECT @l_v_cmd = 'echo cd ' + @l_v_ftp_path + '>> ' + @l_v_workdir + @l_v_work_file_name
    EXEC master..xp_cmdshell @l_v_cmd
  END
  
  --select	@cmd = 'echo '					+ 'put ' + @SourcePath+ @FileName + ' '  + @FTPPath + @FileName
  SELECT @l_v_cmd = 'echo ' + 'put ' + @l_v_source_path + @ri_v_file_name + ' ' + @ri_v_file_name + ' >> ' + @l_v_workdir + @l_v_work_file_name
  --waitfor delay '00:00:05'
  EXEC master..xp_cmdshell @l_v_cmd
  
  SELECT @l_v_cmd = 'echo ' + 'quit' + ' >> ' + @l_v_workdir + @l_v_work_file_name
  EXEC master..xp_cmdshell @l_v_cmd

  SELECT @l_v_cmd = 'ftp -s:' + @l_v_workdir + @l_v_work_file_name
  INSERT #a EXEC master..xp_cmdshell @l_v_cmd

  SET @ri_n_err = ISNULL((SELECT COUNT('') FROM #a a
						  JOIN tb_m_error_interface_file b
							  ON a.s LIKE '%' + b.Patern + '%')
						  , 0)
						  
  
  IF @ri_n_err > 0
  BEGIN
	SELECT @l_v_msg_text = 'send ftp error :' + s
	FROM #a a
	JOIN tb_m_error_interface_file b
		 ON a.s LIKE '%' + b.Patern + '%'
		 
	IF (@ri_v_flag_msg = 'Y')
	BEGIN
		EXEC SP_GET_MESSAGE 'MSPXPR011ERR', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @ri_v_file_name, @l_v_msg_text
		EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR009ERR', 'ERR', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;

		while @l_n_id <= (select count('') from #a)
		begin
	  		select @l_v_msg_text = s from #a where id = @l_n_id
		
			EXEC SP_GET_MESSAGE 'MSPXPR009ERR', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @l_v_msg_text
			EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR009ERR', 'ERR', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;

	  		set @l_n_id = @l_n_id + 1
		end
	END
	SET @ri_n_err = ISNULL(@ri_n_err, 0) + 1
  END
  ELSE
  BEGIN
    EXEC SP_GET_MESSAGE 'MSPXPR013INF', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @ri_v_file_name, @l_v_ftp_ip_address
	EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR013INF', 'INF', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;
  END
  
  DROP TABLE #a
  
  SELECT @l_v_cmd = 'del ' + @l_v_workdir + @l_v_work_file_name
  EXEC master..xp_cmdshell @l_v_cmd
END


GO



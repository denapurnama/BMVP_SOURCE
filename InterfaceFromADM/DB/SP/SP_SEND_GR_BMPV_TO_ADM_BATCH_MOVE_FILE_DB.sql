CREATE PROCEDURE [dbo].[SP_SEND_GR_BMPV_TO_ADM_BATCH_MOVE_FILE_DB] 
	@ri_n_process_id		bigint,
	@ri_v_module_id			varchar(20),
	@ri_v_function_id		varchar(20), 
	@ri_v_source_path		varchar(max), 
	@ri_v_destination_path	varchar(max), 
	@ri_v_file_name			varchar(100), 
	@ri_v_user_id			varchar(20),
	@ri_n_err				int OUT, 
	@ri_b_is_timestamp		bit = NULL
AS
BEGIN
  DECLARE 
		@l_v_cmd				nvarchar(4000),
        @l_v_user_name			varchar(20) = 'system',
        @l_v_location			varchar(100) = 'move_interface_file_db',
        @l_v_business_err_key	varchar(20) = '##ERROR_BUSINESS##',
		@l_v_process_status     varchar(1) = '0',
		@l_v_log_position		varchar(max) = 'Moving File in DB Server',
		@l_v_log_mesg_id		varchar(12),
		@l_v_log_mesg			varchar(max),
		@l_v_msg_text			varchar(max),
		@l_v_new_file_name		varchar(100),
		@l_n_return_value		tinyint = 0,
		@l_n_process_status     tinyint = 0,
		@l_b_have_warning_log	bit = 0
		--@L_ERR int

	CREATE TABLE #b (
		id int IDENTITY (1, 1),
		s varchar(1000)
	)

  IF @ri_b_is_timestamp = 1
  BEGIN
    IF @ri_v_file_name LIKE '%.CTF'
    BEGIN
      SET @l_v_new_file_name = LEFT(@ri_v_file_name, LEN(@ri_v_file_name) - 4) + '.' + REPLACE(REPLACE(REPLACE(CONVERT(varchar(19), GETDATE(), 121), '-', ''), ':', ''), ' ', '') + '.ctf'
      SET @l_v_cmd = 'move ' + @ri_v_source_path + @ri_v_file_name + ' ' + @ri_v_destination_path + @l_v_new_file_name
    END
    ELSE IF @ri_v_file_name LIKE '%.xlsx'
    BEGIN
      SET @l_v_new_file_name = LEFT(@ri_v_file_name, LEN(@ri_v_file_name) - 5) + '.' + REPLACE(REPLACE(REPLACE(CONVERT(varchar(19), GETDATE(), 121), '-', ''), ':', ''), ' ', '') + '.xlsx'
      SET @l_v_cmd = 'move ' + @ri_v_source_path + @ri_v_file_name + ' ' + @ri_v_destination_path + @l_v_new_file_name
    END
    ELSE
    BEGIN
      SET @l_v_new_file_name = @ri_v_file_name + '.' + REPLACE(REPLACE(REPLACE(CONVERT(varchar(19), GETDATE(), 121), '-', ''), ':', ''), ' ', '')
      SET @l_v_cmd = 'move ' + @ri_v_source_path + @ri_v_file_name + ' ' + @ri_v_destination_path + @l_v_new_file_name
      --SELECT @stringsql stringsql, @SOURCEPATH, @DESTINATIONPATH, @Files, @newFiles
    END
  END
  ELSE
  BEGIN
    SET @l_v_cmd = 'move ' + @ri_v_source_path + @ri_v_file_name + ' ' + @ri_v_destination_path + @ri_v_file_name
  END

  EXEC SP_PUTLOG @l_v_cmd, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR000INF', 'INF', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;

  INSERT #b
  EXEC master..xp_cmdshell @l_v_cmd
  
  SET @ri_n_err = ISNULL((SELECT COUNT('')
						  FROM #b a
						  JOIN tb_m_error_interface_file b
							ON a.s LIKE '%' + b.Patern + '%')
						  , 0)
						  
  IF @ri_n_err > 0
  BEGIN
    SET @l_v_msg_text = 'Error Move file ' + ISNULL(@ri_v_file_name, '') + ' From ' + ISNULL(@ri_v_source_path, '') + ' To ' + ISNULL(@ri_v_destination_path, '')
    SELECT @l_v_msg_text = @l_v_msg_text + ' :' + s
    FROM #b a
    JOIN tb_m_error_interface_file b
      ON a.s LIKE '%' + b.Patern + '%'

	EXEC SP_GET_MESSAGE 'MSPXPR009ERR', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @l_v_msg_text
	EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR009ERR', 'ERR', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;
	SET @ri_n_err = ISNULL(@ri_n_err, 0) + 1
  END
  ELSE
  BEGIN
	EXEC SP_GET_MESSAGE 'MSPXPR012INF', @l_v_log_mesg OUTPUT, @ri_n_err OUTPUT, @ri_v_file_name, @ri_v_source_path, @ri_v_destination_path
	EXEC SP_PUTLOG @l_v_log_mesg, @ri_v_user_id, @l_v_log_position, @ri_n_process_id OUTPUT, 'MSPXPR012INF', 'INF', @ri_v_module_id, @ri_v_function_id, @l_n_process_status;
  END
  
  DROP TABLE #b
END


GO



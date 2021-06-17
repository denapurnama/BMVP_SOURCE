CREATE PROCEDURE [dbo].[SP_SEND_GR_BMPV_TO_ADM_BATCH_GET_FILE_SIZE] 
	@ri_v_path		varchar(1000),
	@ri_v_file_name	varchar(1000),
	@ri_v_size		varchar(100) out
AS
BEGIN
	DECLARE @l_v_fs		varchar(200),
			@l_n_pos	int,
			@l_v_cmd	varchar(1000)
	CREATE TABLE #tempsize (fs varchar(200))
	
	set nocount on
	set @l_v_cmd = 'dir "' + @ri_v_path + @ri_v_file_name + '"'
	Insert #tempsize exec master..xp_cmdshell @l_v_cmd
	
	select @l_v_fs = fs 
	from #tempsize
	where upper (fs) like '%'+ @ri_v_file_name +'%'
	
	set @l_n_pos = CharIndex (('M'), @l_v_fs, 1);
	set @ri_v_size = rtrim(ltrim(substring (@l_v_fs , @l_n_pos + 1, 18 )))
	
	drop table #tempsize
end


GO



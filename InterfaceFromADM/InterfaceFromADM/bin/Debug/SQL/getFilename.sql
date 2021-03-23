declare @@type varchar(10),
		@@systemSource varchar(10),
		@@date varchar(12),
		@@seq varchar(5)

SELECT @@type = SYSTEM_VALUE FROM TB_M_SYSTEM WHERE SYSTEM_CD = 'TP_IPPCS_ACTIVITY'
SELECT @@systemSource = SYSTEM_VALUE FROM TB_M_SYSTEM WHERE SYSTEM_CD = 'SYSTEM_SOURCE_GR'
select @@date = replace(convert(varchar(10), getdate(), 102),'.','')
SELECT @@seq = (REPLICATE('0', 4 - LEN(SYSTEM_VALUE))+ SYSTEM_VALUE) FROM TB_M_SYSTEM WHERE SYSTEM_CD = 'TP_SEQ_NUMBER' -- create 4 Digit sequence

select @@type +'_'+ @@systemSource +'_'+ @@date + @@seq
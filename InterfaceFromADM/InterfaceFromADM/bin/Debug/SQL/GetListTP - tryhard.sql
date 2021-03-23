﻿select 
'000006' as PROCESS_ID ,
 '123'as PROCESS_KEY,
'IPPCS'as SYSTEM_SOURCE,
	
'dummt'as CLIENT_ID,
		
'301'as MOVEMENT_TYPE,

'10.06.2020' DOC_DT, -- change format to dd.mm.yyyy
		
'10.06.2020' AS POSTING_DT, -- change format to dd.mm.yyyy
		
'A021'as REF_NO,

'dummy'as MAT_DOC_DESC,
'019990K417'as SND_PART_NO,

'D'as SND_PROD_PURPOSE_CD,

'3'as SND_SOURCE_TYPE,

'1000'as SND_PLANT_CD,
'1400'as SND_SLOC_CD,
'1000'as SND_BATCH_NO,
'019990K417'as RCV_PART_NO,

'D'as RCV_PROD_PURPOSE_CD,

'3'as RCV_SOURCE_TYPE,
'1000'as RCV_PLANT_CD,
'1400'as RCV_SLOC_CD,
'1000'as RCV_BATCH_NO,
'150000'as QUANTITY,
'RP'as UOM,
'Y'DN_COMPLETE_FLAG, -- default value =Y
'ABI'as CREATED_BY,
'10.06.2020' as CREATED_DT,

'Y' as DN_COMPLETE_FLAG -- default value =Y
﻿DECLARE @@SEQ INT
SELECT @@SEQ = cast(SYSTEM_VALUE AS INT) FROM TB_M_SYSTEM WHERE SYSTEM_CD = 'TP_SEQ_NUMBER'
SET @@SEQ = @@SEQ + 1

UPDATE TB_M_SYSTEM
SET
	SYSTEM_VALUE = CAST(@@SEQ AS VARCHAR(4))
WHERE SYSTEM_CD = 'TP_SEQ_NUMBER'
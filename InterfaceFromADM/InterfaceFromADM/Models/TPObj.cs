using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace InterfaceFromADM.Models
{
    public class TPObj
    {
        public String PROCESS_ID { get; set; }
        public String PROCESS_KEY { get; set; }
        public String SYSTEM_SOURCE { get; set; }
        public String CLIENT_ID { get; set; }
        public String MOVEMENT_TYPE { get; set; }
        public String DOC_DT { get; set; }
        public String POSTING_DT { get; set; }
        public String REF_NO { get; set; }
        public String MAT_DOC_DESC { get; set; }
        public String SND_PART_NO { get; set; }
        public String SND_PROD_PURPOSE_CD { get; set; }
        public String SND_SOURCE_TYPE { get; set; }
        public String SND_PLANT_CD { get; set; }
        public String SND_SLOC_CD { get; set; }
        public String SND_BATCH_NO { get; set; }
        public String RCV_PART_NO { get; set; }
        public String RCV_PROD_PURPOSE_CD { get; set; }
        public String RCV_SOURCE_TYPE { get; set; }
        public String RCV_PLANT_CD { get; set; }
        public String RCV_SLOC_CD { get; set; }
        public String RCV_BATCH_NO { get; set; }
        public String QUANTITY { get; set; }
        public String UOM { get; set; }
        public String DN_COMPLETE_FLAG { get; set; }
        public String CREATED_BY { get; set; }
        public String CREATED_DT { get; set; }
    }

    public class SysvalObj
    {
        public String DOCK_SERVICE_PART { get; set; }
        public String TP_SPLR_CODE { get; set; }
        public String SYSTEM_SOURCE_GR { get; set; }
        public String PROD_PURPOSE_CD_GR { get; set; }
        public String PROD_PURPOSE_GR_SERVICE_PART { get; set; }
        public String SOURCE_TYPE_TP { get; set; }
        public String MAT_DOC_DESC { get; set; }
        public String ORI_UNIT_MEASU_CD_GR { get; set; }
        public String POST_TIME_FLTR { get; set; }
        public String POST_LGCL_FLTR { get; set; }
        public String POST_FLG_FLTR { get; set; }
        public int TYPE_CODE { get; set; }
        public String clientId { get; set; }
    }

    public class SendICSRes
    {
        public string isResult { get; set; }
    }

    public class FormFile
    {
        public string Name { get; set; }

        public string ContentType { get; set; }

        public string FilePath { get; set; }

        public Stream Stream { get; set; }
    }

    public class response
    {
        public string PROCESS_ID { get; set; }
        public string MESSAGE_TEXT { get; set; }
    }
}

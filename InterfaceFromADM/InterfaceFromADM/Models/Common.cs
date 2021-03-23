using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace InterfaceFromADM.Models
{
    public class Common
    {
        public string FUNCTION_ID { get; set; }
        public string SYSTEM_CD { get; set; }
        public string SYSTEM_VALUE { get; set; }
        public string SYSTEM_REMARK { get; set; }

        public string MESSAGE_ID { get; set; }
        public string MESSAGE_TEXT { get; set; }
        public string MESSAGE_TYPE { get; set; }

        public Int64 PID { get; set; }
        public string MSG_TXT { get; set; }
        public string USER_ID { get; set; }
        public string LOCATION { get; set; }
        public string MSG_ID { get; set; }
        public string MSG_TYPE { get; set; }
        public string MODULE_ID { get; set; }
        public int PROCESS_STS { get; set; }

        public Int32 ROW_NUM { get; set; }
        public string ITEM { get; set; }
        public string PROCESS_ID { get; set; }
        public string PROCESS_NAME { get; set; }
        public string ErrorEx { get; set; }
        public string MSG_TEXT { get; set; }
        public string MSG_TEXT2 { get; set; }
        public string MSG_TEXT3 { get; set; }
        public string ERR_LOC { get; set; }

        public const string VALUE_SUCCESS = "SUCCESS";
        public const string VALUE_ERROR = "ERROR";

        public string Result { set; get; }
        public string ProcessId { set; get; }
        public string[] SuccMesgs { set; get; }
        public string[] ErrMesgs { set; get; }
        public object[] Params { set; get; }
        public object[] AddParams { set; get; }
        
    }

    public class FTPCredential
    {
        public String HOST_NAME { get; set; }
        public String USER_NAME { get; set; }
        public String PASSWORD { get; set; }
        public String HOST_NAME_SUCCESS { get; set; }
        public String HOST_NAME_FAILED { get; set; }
        public string ITEM { get; set; }
    }

    public class SupplierParameter
    {
        public Int32 V_SEQ_NO { get; set; }
        public String V_TMMIN_QD_INVC_NO { get; set; }
        public String D_TMMIN_PROC_MONTH { get; set; }
        public String N_PROC_SEQ { get; set; }
        public String V_TMAPSUPP_INVC_NO { get; set; }
    }

    public class NettingParameter
    {
        public Int32 V_SEQ_NO { get; set; }
        public String V_QD_NOTE_NO { get; set; }
        public String D_PROC_MONTH { get; set; }
        public String V_TMAPSUPP_NOTE_NO { get; set; }
        public String V_WBS_NO { get; set; }
    }

    public class EmailContent
    {
        public String V_EMAIL_CD { get; set; }
        public Int32 N_SEQ_NO { get; set; }
        public String V_MAIL_TO { get; set; }
        public String V_MAIL_CC { get; set; }
        public String V_MAIL_SUBJECT { get; set; }
        public String V_MAIL_BODY { get; set; }
        public String V_ATTACH_FILE { get; set; }
    }

    public class ListVPH
    {
        public string D_CRE_DT { get; set; }
        public string V_LINE_CODE { get; set; }
        public string V_EG_TYPE { get; set; }
        public string V_TRANS_TYPE { get; set; }
        public string V_DEST { get; set; }
        public string V_FRAME_TYPE { get; set; }
        public string V_FRAME_NO { get; set; }
        public string V_VIN_WMI { get; set; }
        public string V_VIN_VDS { get; set; }
        public string V_VIN_VIS { get; set; }
        public string V_MDL_YEAR { get; set; }
        public string V_CHK_DGT { get; set; }
        public string V_ID_NO { get; set; }
        public string V_SPEC_SHEET_NO { get; set; }
        public string V_SSR_CODE { get; set; }
        public string V_SMS_CFC { get; set; }
        public string V_KTSHK_CODE { get; set; }
        public string V_KTSHK { get; set; }
        public string V_LO_KTSHK_CODE { get; set; }
        public string V_LO_KTSHK { get; set; }
        public string Blank { get; set; }
        public string V_CTRL_KTSHK { get; set; }
        public string V_EXT_COLOR { get; set; }
        public string V_INT_COLOR { get; set; }
        public string V_PROD_SPEC { get; set; }
        public string V_PROD_CC { get; set; }
        public string V_OVS_PLANT_CODE { get; set; }
        public string V_ASMBLY_LINE { get; set; }
        public string V_TMCR { get; set; }
        public string V_TMCS { get; set; }
        public string D_PLO_DATE { get; set; }
        public string D_ALO_DATE { get; set; }
        public string D_FI_DATE { get; set; }
        public string V_KNI { get; set; }
        public string V_KND { get; set; }
        public string V_ABND { get; set; }
        public string V_ABNP { get; set; }
        public string V_EG_NO { get; set; }
        public string Blank2 { get; set; }
        public string V_WFS { get; set; }
        public string V_ABNSFR { get; set; }
        public string V_ABNSFL { get; set; }
        public string V_ABNRSR { get; set; }
        public string V_ABNRSL { get; set; }
        public string V_ABNCFR { get; set; }
        public string V_ABNCFL { get; set; }
        public string V_ABNKR { get; set; }
        public string V_ABNKL { get; set; }
        public string V_FILLER { get; set; }
        public string V_TRANSMISSION_TYPE { get; set; }
        public string Blank3 { get; set; }
    }
}

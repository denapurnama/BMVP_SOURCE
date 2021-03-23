using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using InterfaceFromADM.Models;
using InterfaceFromADM.Helper.DBConfig;
using InterfaceFromADM.Helper.FTP;
using InterfaceFromADM.Helper.Base;
using System.IO;
using System.Reflection;
using Toyota.Common.Database;
using Toyota.Common.Web.Platform;
using System.IO.Compression;
using InterfaceFromADM.Helper.Util;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;

namespace InterfaceFromADM.AppCode
{
    public class InterfaceFromADM : BaseBatch
    {
        #region Batch Start
        public override void ExecuteBatch()
        {
            string loc = "Interface File From ADM Batch";
            string module = "BMPV";
            string function = "IFilePriceFromADM";

            Common repoResult = null;
            Common getProc = new Common();
            getProc.MSG_TXT = "Start Process";
            getProc.LOCATION = loc;
            getProc.PID = 0;
            getProc.MSG_ID = "ADMIPPCS1INF";
            getProc.MSG_TYPE = "INF";
            getProc.MODULE_ID = module;
            getProc.FUNCTION_ID = function;
            getProc.USER_ID = "SYSTEM";
            getProc.PROCESS_STS = 0;
            Int64 PID = CommonDBHelper.Instance.CreateLog(getProc);

            CommonDBHelper Repo = CommonDBHelper.Instance;

            Console.WriteLine("Function is started");
            try
            {
                IDBContext db = dbManager.GetContext();

                #region 1. Get File From Receive Folder
                Console.WriteLine("1. Get File From Receive Folder");

                getProc.MSG_TXT = "Get File From Receive Folder";
                getProc.LOCATION = loc;
                getProc.PID = PID;
                getProc.MSG_ID = "ADMIPPCS1INF";
                getProc.MSG_TYPE = "INF";
                getProc.MODULE_ID = module;
                getProc.FUNCTION_ID = function;
                getProc.PROCESS_STS = 0;
                getProc.USER_ID = "SYSTEM";
                PID = CommonDBHelper.Instance.CreateLogDetail(getProc);

                int totalSuccess = 0;
                string val = sysValue();
                string fileName;
                string fileSys = "ADM_Interface";

                String[] listfile = Directory.GetFiles(val);
                fileName = listfile[0].Substring(8, 13);

                if (fileName != fileSys)
                {
                    getProc.MSG_TXT = "Failed to get file in the directory";
                    getProc.LOCATION = loc;
                    getProc.PID = PID;
                    getProc.MSG_ID = "ADMIPPCS1INF";
                    getProc.MSG_TYPE = "ERR";
                    getProc.MODULE_ID = module;
                    getProc.FUNCTION_ID = function;
                    getProc.PROCESS_STS = 0;
                    getProc.USER_ID = "SYSTEM";
                    PID = CommonDBHelper.Instance.CreateLogDetail(getProc);

                    return;
                }

                #endregion


                #region 2. Read File and Load Into Staging
                try
                {
                    if (listfile != null)
                    {
                        Console.WriteLine("2. Read File and Load Into Staging");

                        getProc.MSG_TXT = "Read File and Load Into Staging Table";
                        getProc.LOCATION = loc;
                        getProc.PID = PID;
                        getProc.MSG_ID = "ADMIPPCS1INF";
                        getProc.MSG_TYPE = "INF";
                        getProc.MODULE_ID = module;
                        getProc.FUNCTION_ID = function;
                        getProc.PROCESS_STS = 0;
                        getProc.USER_ID = "SYSTEM";
                        PID = CommonDBHelper.Instance.CreateLogDetail(getProc);

                        #region Create Data Table
                        DataTable table = new DataTable();

                        table.Columns.Add("WARP_BUYER_CD");
                        table.Columns.Add("SOURCE_DATA");
                        table.Columns.Add("DRAFT_DF");
                        table.Columns.Add("WARP_REF_NO");
                        table.Columns.Add("CPP_FLAG");
                        //table.Columns.Add("ID");
                        table.Columns.Add("CREATED_BY");
                        table.Columns.Add("CREATED_DT");


                        table.Columns.Add("MAT_NO");
                        table.Columns.Add("PROD_PURPOSE_CD");
                        table.Columns.Add("SOURCE_TYPE");
                        table.Columns.Add("SUPP_CD");
                        table.Columns.Add("PART_COLOR_SFX");
                        table.Columns.Add("PACKING_TYPE");
                        table.Columns.Add("PRICE_STATUS");
                        table.Columns.Add("PRICE_AMT");
                        table.Columns.Add("CURR_CD");
                        table.Columns.Add("VALID_DT_FR");
                        table.Columns.Add("VALID_DT_TO");

                        #endregion

                        int FILE_NO = 1;
                        foreach (var filename in listfile)
                        {
                            try
                            {
                                #region Read file's content and add into data table
                                using (StreamReader reader = new StreamReader(filename))
                                {
                                    string RowData;
                                    string[] data;


                                    int SEQ = 1;
                                    while ((RowData = reader.ReadLine()) != null)
                                    {
                                        DataRow row = table.NewRow();

                                        data = RowData.Split(new string[] { "\t" }, StringSplitOptions.None);


                                        row["WARP_BUYER_CD"] = "";
                                        row["SOURCE_DATA"] = "";
                                        row["DRAFT_DF"] = "";
                                        row["WARP_REF_NO"] = "";
                                        row["CPP_FLAG"] = "N";
                                        //row["ID"] = ;
                                        row["CREATED_BY"] = "SYSTEM";
                                        row["CREATED_DT"] = DateTime.Now;

                                        row["MAT_NO"] = data[0];
                                        row["PROD_PURPOSE_CD"] = data[1];
                                        row["SOURCE_TYPE"] = data[2];
                                        row["SUPP_CD"] = data[3];
                                        row["PART_COLOR_SFX"] = data[4];
                                        row["PACKING_TYPE"] = data[5];
                                        row["PRICE_STATUS"] = data[6];
                                        row["PRICE_AMT"] = data[7];
                                        row["CURR_CD"] = data[8];
                                        row["VALID_DT_FR"] = data[9].Contains("-") ? data[9] : data[9].Insert(4, "-").Insert(7, "-");
                                        row["VALID_DT_TO"] = data[10].Contains("-") ? data[10] : data[10].Insert(4, "-").Insert(7, "-");

                                        table.Rows.Add(row);
                                        SEQ++;
                                    }
                                    totalSuccess = totalSuccess + 1;
                                }
                                #endregion
                            }
                            catch (Exception ex)
                            {

                                getProc.MSG_TXT = "Error Import Data From File " + filename;
                                getProc.LOCATION = loc;
                                getProc.PID = PID;
                                getProc.MSG_ID = "ADMIPPCS1INF";
                                getProc.MSG_TYPE = "ERR";
                                getProc.MODULE_ID = module;
                                getProc.FUNCTION_ID = function;
                                getProc.PROCESS_STS = 0;
                                getProc.USER_ID = "SYSTEM";
                                PID = CommonDBHelper.Instance.CreateLogDetail(getProc);

                                MoveFileToArchieveFailed();

                                return;
                            }
                            FILE_NO++;
                        }


                        #region Save Temp Data into Database
                        if (totalSuccess > 0)
                        {
                            string del = deleteStaging();
                            if (del != "SUCCESS")
                            {
                                throw new Exception(del);
                            }
                            else
                            {
                                string msg = SaveTemp(table);
                                if (msg != "SUCCESS")
                                    throw new Exception(msg);
                            }
                        }
                        #endregion

                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("error");
                    getProc.MSG_TXT = "Error Import Data From File Price";
                    getProc.LOCATION = loc;
                    getProc.PID = PID;
                    getProc.MSG_ID = "ADMIPPCS1INF";
                    getProc.MSG_TYPE = "ERR";
                    getProc.MODULE_ID = module;
                    getProc.FUNCTION_ID = function;
                    getProc.PROCESS_STS = 0;
                    getProc.USER_ID = "SYSTEM";
                    PID = CommonDBHelper.Instance.CreateLogDetail(getProc);

                    MoveFileToArchieveFailed();

                    return;
                }
                #endregion
            }
            catch (Exception ex)
            {
                Console.WriteLine("error");
                getProc.MSG_TXT = "Error Get file from receive folder";
                getProc.LOCATION = loc;
                getProc.PID = PID;
                getProc.MSG_ID = "ADMIPPCS1INF";
                getProc.MSG_TYPE = "ERR";
                getProc.MODULE_ID = module;
                getProc.FUNCTION_ID = function;
                getProc.PROCESS_STS = 0;
                getProc.USER_ID = "SYSTEM";
                PID = CommonDBHelper.Instance.CreateLogDetail(getProc);

                //MoveFileToArchieveFailed();

                return;
            }

            repoResult = Operation(PID);
            if (repoResult.ErrMesgs[0].Equals(Common.VALUE_SUCCESS))
            {
                MoveFileToArchieveSuccess();
            }
            else
            {
                MoveFileToArchieveFailed();
            }
            #region Move File After Operation

            #endregion
        }
        #endregion

        #region Get File Location Temp
        public string sysValue()
        {
            string result;
            IDBContext db = dbManager.GetContext();

            dynamic args = new
            {

            };

            result = db.SingleOrDefault<string>("Sysvalue", args);
            return result;
        }
        #endregion

        #region Delete Staging Table
        public string deleteStaging()
        {
            IDBContext db = dbManager.GetContext();

            db.Execute("DELETE FROM TB_T_DRAFT_MATERIAL_PRICE");
            return "SUCCESS";
        }
        #endregion

        #region Insert to Staging Table
        public string SaveTemp(DataTable data)
        {

            //string consString = System.Configuration.ConfigurationManager.ConnectionStrings[DB].ConnectionString;
            using (SqlConnection conn = new SqlConnection(connDesc.ConnectionString))
            //using (SqlConnection conn = new SqlConnection(
            //        "Data Source=192.168.0.20;" +
            //        "Initial Catalog=IPPCS_QA;" +
            //        "User id=ippcs_users;" +
            //        "Password=ippcsuser123;")
            //       )
            {
                using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(conn))
                {
                    conn.Open();

                    sqlBulkCopy.DestinationTableName = "TB_T_DRAFT_MATERIAL_PRICE";
                    try
                    {
                        sqlBulkCopy.ColumnMappings.Add("WARP_BUYER_CD", "WARP_BUYER_CD");
                        sqlBulkCopy.ColumnMappings.Add("SOURCE_DATA", "SOURCE_DATA");
                        sqlBulkCopy.ColumnMappings.Add("DRAFT_DF", "DRAFT_DF");
                        sqlBulkCopy.ColumnMappings.Add("WARP_REF_NO", "WARP_REF_NO");
                        sqlBulkCopy.ColumnMappings.Add("CPP_FLAG", "CPP_FLAG");
                        //sqlBulkCopy.ColumnMappings.Add("ID", "ID");
                        sqlBulkCopy.ColumnMappings.Add("CREATED_BY", "CREATED_BY");
                        sqlBulkCopy.ColumnMappings.Add("CREATED_DT", "CREATED_DT");

                        sqlBulkCopy.ColumnMappings.Add("MAT_NO", "MAT_NO");
                        sqlBulkCopy.ColumnMappings.Add("PROD_PURPOSE_CD", "PROD_PURPOSE_CD");
                        sqlBulkCopy.ColumnMappings.Add("SOURCE_TYPE", "SOURCE_TYPE");
                        sqlBulkCopy.ColumnMappings.Add("SUPP_CD", "SUPP_CD");
                        sqlBulkCopy.ColumnMappings.Add("PART_COLOR_SFX", "PART_COLOR_SFX");
                        sqlBulkCopy.ColumnMappings.Add("PACKING_TYPE", "PACKING_TYPE");
                        sqlBulkCopy.ColumnMappings.Add("PRICE_STATUS", "PRICE_STATUS");
                        sqlBulkCopy.ColumnMappings.Add("PRICE_AMT", "PRICE_AMT");
                        sqlBulkCopy.ColumnMappings.Add("CURR_CD", "CURR_CD");
                        sqlBulkCopy.ColumnMappings.Add("VALID_DT_FR", "VALID_DT_FR");
                        sqlBulkCopy.ColumnMappings.Add("VALID_DT_TO", "VALID_DT_TO");

                        sqlBulkCopy.WriteToServer(data);

                        conn.Close();
                        return "SUCCESS";
                    }
                    catch (Exception ex)
                    {
                        conn.Close();
                        return ex.Message;
                    }
                }
            }
        }

        #endregion

        #region Operation
        public Common Operation(Int64 p_id)
        {
            SqlParameter outputRetVal = CreateSqlParameterOutputReturnValue("RetVal");
            SqlParameter outputErrMesg = CreateSqlParameterOutputErrMesg("ErrMesg");

            IDBContext db = dbManager.GetContext();

            dynamic args = new
            {
                PROCESS_ID = p_id,
                RetVal = outputRetVal,
                ErrMesg = outputErrMesg
            };

            int result = db.Execute("Execute_SP", args);
            Common Rresult = new Common();
            Rresult.Result = Common.VALUE_ERROR;
            string errMesg = string.Empty;
            if (outputErrMesg != null && outputErrMesg.Value != null)
            {
                errMesg = outputErrMesg.Value.ToString();
            }
            Rresult.ErrMesgs = new string[1];
            Rresult.ErrMesgs[0] = errMesg;
            return Rresult;
        }

        protected SqlParameter CreateSqlParameterOutputReturnValue(string parameterName)
        {
            var outputRetVal = new SqlParameter(parameterName, System.Data.SqlDbType.Int);
            outputRetVal.Direction = System.Data.ParameterDirection.Output;
            outputRetVal.Value = -1;

            return outputRetVal;
        }

        protected SqlParameter CreateSqlParameterOutputErrMesg(string parameterName)
        {
            var outputErrMesg = new System.Data.SqlClient.SqlParameter(parameterName, System.Data.SqlDbType.VarChar, 2000);
            outputErrMesg.Direction = System.Data.ParameterDirection.Output;
            outputErrMesg.Value = string.Empty;

            return outputErrMesg;
        }

        public override List<Common> GetList()
        {
            throw new NotImplementedException();
        }
        #endregion

        #region Move File after operation

        #region Archive Success
        public string ArchiveSuccess()
        {
            string result;
            IDBContext db = dbManager.GetContext();

            dynamic args = new
            {

            };

            result = db.SingleOrDefault<string>("File_Archive_Success", args);
            return result;
        }
        #endregion

        #region Archive Failed
        public string ArchiveFailed()
        {
            string result;
            IDBContext db = dbManager.GetContext();

            dynamic args = new
            {

            };

            result = db.SingleOrDefault<string>("File_Archive_Failed", args);
            return result;
        }
        #endregion

        #region MoveFileToArchieveSuccess
        public string MoveFileToArchieveSuccess()
        {
            string fileFromTemp = sysValue();
            string destArchiveSuccess = ArchiveSuccess();

            try
            {
                String[] listFile = Directory.GetFiles(fileFromTemp, "*.txt");

                // Copy files.
                foreach (string f in listFile)
                {

                    // Remove path from the file name.
                    string fName = f.Substring(fileFromTemp.Length + 1);
                    string fNameNew = (DateTime.Now.ToString("yyyy-MM-dd HH.mm.ss") + "_" + f.Substring(fileFromTemp.Length + 1));
                    try
                    {
                        // Will not overwrite if the destination file already exists.
                        //File.Copy(Path.Combine(fileFromTemp, fName), Path.Combine(destArchiveSuccess, fName));
                        File.Copy(Path.Combine(fileFromTemp, fName), Path.Combine(destArchiveSuccess, fNameNew));
                    }

                    // Catch exception if the file was already copied.
                    catch (IOException copyError)
                    {
                        Console.WriteLine(copyError.Message);
                    }
                }

                // Delete source files that were copied.
                foreach (string f in listFile)
                {
                    File.Delete(f);
                }

            }
            catch (DirectoryNotFoundException dirNotFound)
            {
                Console.WriteLine(dirNotFound.Message);
            }

            return "SUCCESS";
        }

        #endregion

        #region MoveFileToArchieveFailed
        public string MoveFileToArchieveFailed()
        {
            string fileFromTemp = sysValue();
            string destArchiveFailed = ArchiveFailed();

            try
            {
                String[] listFile = Directory.GetFiles(fileFromTemp);

                // Copy files.
                foreach (string f in listFile)
                {

                    // Remove path from the file name.
                    string fName = f.Substring(fileFromTemp.Length + 1);
                    string fNameNew = (DateTime.Now.ToString("yyyy-MM-dd HH.mm.ss") + "_" + f.Substring(fileFromTemp.Length + 1));
                    try
                    {
                        // Will not overwrite if the destination file already exists.
                        //File.Copy(Path.Combine(fileFromTemp, fName), Path.Combine(destArchiveFailed, fName));
                        File.Copy(Path.Combine(fileFromTemp, fName), Path.Combine(destArchiveFailed, fNameNew));
                    }

                    // Catch exception if the file was already copied.
                    catch (IOException copyError)
                    {
                        Console.WriteLine(copyError.Message);
                    }
                }

                // Delete source files that were copied.
                foreach (string f in listFile)
                {
                    File.Delete(f);
                }

            }
            catch (DirectoryNotFoundException dirNotFound)
            {
                Console.WriteLine(dirNotFound.Message);
            }

            return "SUCCESS";
        }
        #endregion

        #endregion
    }
}
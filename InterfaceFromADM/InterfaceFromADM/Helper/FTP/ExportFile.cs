using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Toyota.Common.Database;
using Toyota.Common.Web.Platform;
using System.Net;
using System.IO;
using InterfaceFromADM.Helper.DBConfig;
using InterfaceFromADM.Models;

namespace InterfaceFromADM.Helper.FTP
{
    public class ExportFile
    {
        public static void SendDatatoFTP(string localPath, string id)
        {
            string HOST_NAME = "-";
            string USER_NAME = "-";
            string PASSWORD = "-";

            CommonDBHelper Repo = CommonDBHelper.Instance;

            List<FTPCredential> data = Repo.GetFtpCredential(id, "GetFtpCredential").ToList();
            foreach (FTPCredential row in data)
            {
                HOST_NAME = row.HOST_NAME;
                USER_NAME = row.USER_NAME;
                PASSWORD = row.PASSWORD;
            }

            NetworkCredential cred = new NetworkCredential(USER_NAME, PASSWORD);

            FtpWebRequest request = (FtpWebRequest)WebRequest.Create(HOST_NAME + Path.GetFileName(localPath));
            request.Proxy = new WebProxy();
            request.Method = WebRequestMethods.Ftp.UploadFile;

            request.Credentials = new NetworkCredential(USER_NAME, PASSWORD);

            StreamReader source = new StreamReader(localPath);
            byte[] fileContent = Encoding.UTF8.GetBytes(source.ReadToEnd());
            source.Close();
            request.ContentLength = fileContent.Length;

            Stream requestStream = request.GetRequestStream();
            requestStream.Write(fileContent, 0, fileContent.Length);
            requestStream.Close();

            FtpWebResponse response = (FtpWebResponse)request.GetResponse();

            response.Close();

            File.Delete(localPath);

        }
    }
}

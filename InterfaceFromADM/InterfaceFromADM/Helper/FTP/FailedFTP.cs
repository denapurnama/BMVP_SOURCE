using InterfaceFromADM.Helper.DBConfig;
using InterfaceFromADM.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace InterfaceFromADM.Helper.FTP
{
    public class FailedFTP
    {
        public static void FTPFailed(string param, string id)
        {
            string HOST_NAME = "-";
            string USER_NAME = "-";
            string PASSWORD = "-";
            string HOST_NAME_FAILED = "-";

            CommonDBHelper Repo = CommonDBHelper.Instance;

            List<FTPCredential> data = Repo.GetFtpCredentialSucc(param, id, "GetFtpDownloadCredential").ToList();
            foreach (FTPCredential row in data)
            {
                HOST_NAME = row.HOST_NAME;
                HOST_NAME_FAILED = row.HOST_NAME_FAILED;
                USER_NAME = row.USER_NAME;
                PASSWORD = row.PASSWORD;
            }

            NetworkCredential cred = new NetworkCredential(USER_NAME, PASSWORD);

            WebClient request = new WebClient();

            request.Credentials = new NetworkCredential(USER_NAME, PASSWORD);

            List<DirectoryItem> listing = GetDirectoryInformation(HOST_NAME, USER_NAME, PASSWORD);//"ftp://ftp.mywebsite.com/directory", "username", "password");

            string name = listing[0].Name;

            //CopyFile(HOST_NAME + name, HOST_NAME_FAILED + name, USER_NAME, PASSWORD);

            System.Net.FtpWebRequest clsRequest = (System.Net.FtpWebRequest)System.Net.WebRequest.Create(HOST_NAME + name);
            clsRequest.Credentials = new System.Net.NetworkCredential(USER_NAME, PASSWORD);
            clsRequest.Method = System.Net.WebRequestMethods.Ftp.DeleteFile;

            System.Net.FtpWebResponse resp = (FtpWebResponse)clsRequest.GetResponse();
            resp.Close();

            //byte[] filedata = request.DownloadData(HOST_NAME + name); //Path.GetFileName(filename));

            //using (FileStream file = File.Create(filename + name))
            //{
            //    file.Write(filedata, 0, filedata.Length);
            //    file.Close();
            //}

            //return filename + name;

        }

        struct DirectoryItem
        {
            public Uri BaseUri;

            public string AbsolutePath
            {
                get
                {
                    return string.Format("{0}/{1}", BaseUri, Name);
                }
            }

            public DateTime DateCreated;
            public bool IsDirectory;
            public string Name;
            public List<DirectoryItem> Items;
        }

        static List<DirectoryItem> GetDirectoryInformation(string address, string username, string password)
        {
            FtpWebRequest request = (FtpWebRequest)FtpWebRequest.Create(address);
            request.Method = WebRequestMethods.Ftp.ListDirectoryDetails;
            request.Credentials = new NetworkCredential(username, password);
            request.UsePassive = true;
            request.UseBinary = true;
            request.KeepAlive = false;

            List<DirectoryItem> returnValue = new List<DirectoryItem>();
            string[] list = null;

            using (FtpWebResponse response = (FtpWebResponse)request.GetResponse())
            using (StreamReader reader = new StreamReader(response.GetResponseStream()))
            {
                list = reader.ReadToEnd().Split(new string[] { "\r\n" }, StringSplitOptions.RemoveEmptyEntries);
            }

            foreach (string line in list)
            {
                // Windows FTP Server Response Format
                // DateCreated    IsDirectory    Name
                string data = line;
                data = data.Remove(0, 49);

                //// Parse date
                //string date = data.Substring(0, 17);
                //DateTime dateTime = DateTime.Parse(date);
                //data = data.Remove(0, 24);

                //// Parse <DIR>
                //string dir = data.Substring(0, 5);
                //bool isDirectory = dir.Equals("<dir>", StringComparison.InvariantCultureIgnoreCase);
                //data = data.Remove(0, 5);
                //data = data.Remove(0, 10);

                //// Parse name
                //string name = data;

                //// Create directory info
                DirectoryItem item = new DirectoryItem();
                //item.BaseUri = new Uri(address);
                //item.DateCreated = dateTime;
                //item.IsDirectory = isDirectory;
                item.Name = data;

                item.Items = item.IsDirectory ? GetDirectoryInformation(item.AbsolutePath, username, password) : null;

                returnValue.Add(item);
            }

            return returnValue;
        }

        public static bool CopyFile(string fileName, string FileToCopy, string userName, string password)
        {
            try
            {
                FtpWebRequest request = (FtpWebRequest)WebRequest.Create(fileName);
                request.Method = WebRequestMethods.Ftp.DownloadFile;

                request.Credentials = new NetworkCredential(userName, password);
                FtpWebResponse response = (FtpWebResponse)request.GetResponse();
                Stream responseStream = response.GetResponseStream();
                Upload(FileToCopy, ToByteArray(responseStream), userName, password);
                responseStream.Close();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static Byte[] ToByteArray(Stream stream)
        {
            MemoryStream ms = new MemoryStream();
            byte[] chunk = new byte[4096];
            int bytesRead;
            while ((bytesRead = stream.Read(chunk, 0, chunk.Length)) > 0)
            {
                ms.Write(chunk, 0, bytesRead);
            }

            return ms.ToArray();
        }

        public static bool Upload(string FileName, byte[] Image, string FtpUsername, string FtpPassword)
        {
            try
            {
                System.Net.FtpWebRequest clsRequest = (System.Net.FtpWebRequest)System.Net.WebRequest.Create(FileName);
                clsRequest.Credentials = new System.Net.NetworkCredential(FtpUsername, FtpPassword);
                clsRequest.Method = System.Net.WebRequestMethods.Ftp.UploadFile;
                System.IO.Stream clsStream = clsRequest.GetRequestStream();
                clsStream.Write(Image, 0, Image.Length);

                clsStream.Close();
                clsStream.Dispose();
                return true;
            }
            catch
            {
                return false;
            }
        }
    }
}

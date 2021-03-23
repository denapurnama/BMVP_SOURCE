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
    public class ImportFile
    {
        public static string GetDatafromFTP(string filename, string id)
        {
            string HOST_NAME = "-";
            string USER_NAME = "-";
            string PASSWORD = "-";

            CommonDBHelper Repo = CommonDBHelper.Instance;

            List<FTPCredential> data = Repo.GetFtpCredential(id, "GetFtpDownloadCredential").ToList();
            foreach (FTPCredential row in data)
            {
                HOST_NAME = row.HOST_NAME;
                USER_NAME = row.USER_NAME;
                PASSWORD = row.PASSWORD;
            }

            NetworkCredential cred = new NetworkCredential(USER_NAME, PASSWORD);

            WebClient request = new WebClient();

            request.Credentials = new NetworkCredential(USER_NAME, PASSWORD);
            List<DirectoryItem> listing = GetDirectoryInformation(HOST_NAME, USER_NAME, PASSWORD);//"ftp://ftp.mywebsite.com/directory", "username", "password");
            string name = listing[0].Name;

            byte[] filedata = request.DownloadData(HOST_NAME + name); //Path.GetFileName(filename));
            using (FileStream file = File.Create(filename + name))
            {
                file.Write(filedata, 0, filedata.Length);
                file.Close();
            }
            
            return filename + name;

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
    }
}

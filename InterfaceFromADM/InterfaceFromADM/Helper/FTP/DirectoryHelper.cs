using InterfaceFromADM.Helper.Base;
using InterfaceFromADM.Helper.DBConfig;
using InterfaceFromADM.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Toyota.Common.Database;
using Toyota.Common.Web.Platform;

namespace InterfaceFromADM.Helper.FTP
{
    public class DirectoryHelper : BaseRepository<Common>
    {
        #region Singleton
        private DirectoryHelper() { }
        private static DirectoryHelper instance = null;
        public static DirectoryHelper Instance
        {
            get
            {
                if (instance == null)
                {
                    SetConfig();
                    instance = new DirectoryHelper();
                }

                return instance;
            }
        }
        #endregion

        public void MoveSuccess(string sourcePath, Common getFileLoc)
        {
            string fileName = Path.GetFileName(sourcePath);

            string targetPath = ""; // CommonDBHelper.Instance.GetPath(getFileLoc);

            string destFile = System.IO.Path.Combine(targetPath, fileName + "_" + DateTime.Now.ToString("yyyyMMdd-HHMMss"));

            if (!System.IO.Directory.Exists(targetPath))
            {
                System.IO.Directory.CreateDirectory(targetPath);
            }

            System.IO.File.Copy(sourcePath, destFile, true);

            File.Delete(sourcePath);
        }

        public void MoveFailed(string sourcePath, Common getFileLoc)
        {
            string fileName = Path.GetFileName(sourcePath);

            string targetPath = ""; //CommonDBHelper.Instance.GetPath(getFileLoc);

            string destFile = System.IO.Path.Combine(targetPath, fileName + "_" + DateTime.Now.ToString("yyyyMMdd-HHMMss"));

            if (!System.IO.Directory.Exists(targetPath))
            {
                System.IO.Directory.CreateDirectory(targetPath);
            }

            System.IO.File.Copy(sourcePath, destFile, true);

            File.Delete(sourcePath);
        }

        public void DropAllFiles(Common getFileLoc)
        {
            string targetPath = ""; // CommonDBHelper.Instance.GetPath(getFileLoc);

            System.IO.DirectoryInfo di = new DirectoryInfo(targetPath);

            try
            {
                foreach (FileInfo file in di.GetFiles())
                {
                    file.Delete();
                }
            }
            catch (Exception e)
            {
                e.Message.ToString();
            }
        }

        public override List<Common> GetList()
        {
            throw new NotImplementedException();
        }
    }
}

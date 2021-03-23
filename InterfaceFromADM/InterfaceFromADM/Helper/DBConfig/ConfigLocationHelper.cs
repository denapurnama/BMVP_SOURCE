using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace InterfaceFromADM.Helper.DBConfig
{
    class ConfigLocationHelper
    {
        public string SQLStatement
        {
            get
            {
                return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SQL");
            }
        }

        public string Configuration
        {
            get
            {
                return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Configurations");
            }
        }
    }
}

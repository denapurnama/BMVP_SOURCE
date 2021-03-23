using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Toyota.Common.Credential;

namespace InterfaceFromADM.Helper.Base
{
    public abstract class BaseModel
    {
        public BaseModel()
        {
            this.CREATED_DT = DateTime.Now;
            this.CREATED_BY = string.Empty;
        }


        public DateTime CREATED_DT { get; set; }
        public String CREATED_BY { get; set; }
        public String CHANGED_BY { get; set; }
        public DateTime? CHANGED_DT { get; set; }

        public void SetCreator(User user)
        {
            this.CREATED_DT = DateTime.Now;
            this.CREATED_BY = user.Username;
        }


        public void SetModifiers(User user)
        {
            this.CHANGED_DT = DateTime.Now;
            this.CHANGED_BY = user.Username;
        }

    }
}

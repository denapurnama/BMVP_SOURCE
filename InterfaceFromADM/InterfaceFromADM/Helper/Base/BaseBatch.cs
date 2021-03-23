using InterfaceFromADM.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace InterfaceFromADM.Helper.Base
{
    public abstract class BaseBatch : BaseRepository<Common>
    {
        public abstract void ExecuteBatch();
    }

    public abstract class BaseBatchParam
    {
        public abstract void ExecuteBatch(string[] batchParams);
    }
}

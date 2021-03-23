using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Toyota.Common.Database;
using Toyota.Common.Web.Platform;
using System.Data.SqlClient;
using System.Reflection;
using System.Data;
using Toyota.Common.Database.Petapoco;
using InterfaceFromADM.Helper.DBConfig;

namespace InterfaceFromADM.Helper.Base
{
    public abstract class BaseRepository<T> where T : class
    {
        public static PetaPocoContextManager dbManager;
        public static ConnectionDescriptor connDesc;

        protected IDBContext GetContext()
        {
            return GetContext(string.Empty);
        }

        public static void SetConfig()
        {
            var defaultLocation = new ConfigLocationHelper();
            var sqlLoader = new FileSqlLoader(defaultLocation.SQLStatement);
            connDesc = DBContextHelper.Instance.getConfiguration();
            dbManager = new PetaPocoContextManager(new[] { sqlLoader }, new[] { connDesc });
            dbManager.SetContextExecutionMode(DBContextExecutionMode.ByName);
        }

        protected IDBContext GetContext(string key)
        {
            if (!string.IsNullOrEmpty(key))
                dbManager.GetContext(key);
            return dbManager.GetContext();
        }

        protected void CloseContext(IDBContext db)
        {
            db.Close();
        }

        public abstract List<T> GetList();

        //validation
        protected Object checkDBNull(Object obj)
        {
            if (obj == null)
                return DBNull.Value;
            if (obj.GetType() == typeof(string) && obj.Equals(string.Empty))
                return DBNull.Value;
            return obj;
        }

        public void ExecuteCmd(Action<SqlCommand> command)
        {
            var ctr = connDesc.ConnectionString;//DatabaseManager.Instance.GetDefaultConnectionDescriptor().ConnectionString;
            SqlConnection con = new SqlConnection(ctr);
            using (SqlCommand cmd = con.CreateCommand())
            {
                try
                {
                    cmd.Connection.Open();
                    command(cmd);
                }
                finally
                {
                    if (cmd.Connection.State == System.Data.ConnectionState.Open)
                        cmd.Connection.Close();
                }
            }
        }

        public List<T> ExecuteQuery(Action<SqlCommand> command)
        {
            var ctr = connDesc.ConnectionString;//DatabaseManager.Instance.GetDefaultConnectionDescriptor().ConnectionString;
            SqlConnection con = new SqlConnection(ctr);
            SqlDataReader reader;
            using (SqlCommand cmd = con.CreateCommand())
            {
                try
                {
                    Console.WriteLine("\t1 : " + DateTime.Now.ToString());
                    cmd.Connection.Open();
                    command(cmd);
                    reader = cmd.ExecuteReader();
                    Console.WriteLine("\t2 : " + DateTime.Now.ToString());
                    List<T> x = DataReaderMapToList<T>(reader);
                    Console.WriteLine("\t3 : " + DateTime.Now.ToString());
                    return x;
                }
                finally
                {
                    if (cmd.Connection.State == System.Data.ConnectionState.Open)
                        cmd.Connection.Close();
                }
            }
        }

        public List<T> ExecuteQuery2(Action<SqlCommand> command)
        {
            var ctr = connDesc.ConnectionString;//DatabaseManager.Instance.GetDefaultConnectionDescriptor().ConnectionString;
            SqlConnection con = new SqlConnection(ctr);
            SqlDataReader reader;
            using (SqlCommand cmd = con.CreateCommand())
            {
                try
                {
                    Console.WriteLine("\t1 : " + DateTime.Now.ToString());
                    cmd.Connection.Open();
                    command(cmd);
                    reader = cmd.ExecuteReader();
                    Console.WriteLine("\t2 : " + DateTime.Now.ToString());
                    //List<T> x = DataReaderMapToList<T>(reader);
                    //Console.WriteLine("\t3 : " + DateTime.Now.ToString());
                    return null;
                }
                finally
                {
                    if (cmd.Connection.State == System.Data.ConnectionState.Open)
                        cmd.Connection.Close();
                }
            }
        }

        public static List<T> DataReaderMapToList<T>(IDataReader dr)
        {
            List<T> list = new List<T>();
            T obj = default(T);

            // -- Declare field names list for Data Reader --
            var fieldNames = new List<string>();
            // --------------------------------

            while (dr.Read())
            {
                // -- Get all field names of Data Reader --
                if (fieldNames.Count == 0)
                {
                    for (int i = 0; i < dr.FieldCount; i++)
                        fieldNames.Add(dr.GetName(i));
                }
                // --------------------------------

                obj = Activator.CreateInstance<T>();
                foreach (PropertyInfo prop in obj.GetType().GetProperties())
                {
                    // -- Check if the Data Reader field name is not exist in the Model. if not exist, then continue.
                    if (!fieldNames.Contains(prop.Name, StringComparer.OrdinalIgnoreCase))
                        continue;
                    // --------------------------------

                    try
                    {
                        if (!object.Equals(dr[prop.Name], DBNull.Value))
                        {
                            //find the property type
                            Type propertyType = prop.PropertyType;

                            //Convert.ChangeType does not handle conversion to nullable types
                            //if the property type is nullable, we need to get the underlying type of the property
                            var targetType = IsNullableType(prop.PropertyType) ? Nullable.GetUnderlyingType(prop.PropertyType) : prop.PropertyType;

                            //Returns an System.Object with the specified System.Type and whose value is
                            //equivalent to the specified object.
                            object propertyVal = Convert.ChangeType(dr[prop.Name], targetType);


                            prop.SetValue(obj, propertyVal, null);
                        }
                    }
                    catch (IndexOutOfRangeException IOOR)
                    {
                        continue;
                    }
                }
                list.Add(obj);
            }
            return list;
        }

        private static bool IsNullableType(Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition().Equals(typeof(Nullable<>));
        }
    }
}

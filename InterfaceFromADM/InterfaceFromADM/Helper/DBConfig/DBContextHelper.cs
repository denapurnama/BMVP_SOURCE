using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Toyota.Common.Configuration;
using Toyota.Common.Configuration.Binder;
using Toyota.Common.Database;

namespace InterfaceFromADM.Helper.DBConfig
{
    class DBContextHelper
    {
        private DBContextHelper() { }
        private static DBContextHelper instance = null;
        public static DBContextHelper Instance
        {
            get
            {
                if (instance == null)
                {
                    instance = new DBContextHelper();
                }
                return instance;
            }
        }

        public ConnectionDescriptor getConfiguration()
        {
            var defaultLocation = new ConfigLocationHelper();
            ConnectionDescriptor connDesc = new ConnectionDescriptor();

            String binderLabel = "Database";

            ConfigurationCabinet connCab = new ConfigurationCabinet("Application Configuration");

            //string DevCode = ApplicationSettings.Instance.Development.Stage.Code;
            //string DevCode = "PRD";
            String DevCode = GetConfigList(binderLabel);
            IConfigurationBinder iconBin = new DifferentialXmlConfigurationBinder(binderLabel, DevCode, defaultLocation.Configuration);

            connCab.AddBinderAndLoad(iconBin);

            //ApplicationConfigurationCabinet.Instance.AddBinderAndLoad(new DifferentialXmlConfigurationBinder(binderLabel, 
            //ApplicationSettings.Instance.Development.Stage.Code, defaultLocation.Configuration));
            IConfigurationBinder configurationBinder = connCab.GetBinder(binderLabel);
            ConfigurationItem[] configurations = configurationBinder.GetConfigurations();
            if (configurations != null)
            {
                var compositeConfigItem = configurations
                    .FirstOrDefault(config => ((CompositeConfigurationItem)config)
                        .GetItem("IsDefault").Value.ToLower() == "true") as CompositeConfigurationItem;

                if (compositeConfigItem != null)
                {
                    connDesc.ConnectionString = compositeConfigItem.GetItem("ConnectionString").Value;
                    connDesc.ProviderName = compositeConfigItem.GetItem("Provider").Value;
                    connDesc.Name = compositeConfigItem.Key;
                    connDesc.IsDefault = Convert.ToBoolean(compositeConfigItem.GetItem("IsDefault").Value);

                    if (String.IsNullOrEmpty(connDesc.ConnectionString))
                        throw new InvalidOperationException("ConnectionSetting is not properly set.");
                    if (String.IsNullOrEmpty(connDesc.ProviderName))
                        throw new InvalidOperationException("Provider is not properly set.");
                }
                else
                    throw new InvalidOperationException("Database-DEV.config is not properly set.");
            }

            return connDesc;
        }

        public static ConfigurationItem GetSystemConfig(String configKey)
        {
            return GetConfigList(AppStage.None, "System")
                .FirstOrDefault(conf => conf.Key == configKey);
        }

        public static IList<ConfigurationItem> GetConfigList(String appStage, String configFilename)
        {
            var defaultLocation = new ConfigLocationHelper();
            var appConfigCabinet = new ConfigurationCabinet("AppConfigCabinet");
            var xmlConfigBinder = new DifferentialXmlConfigurationBinder(configFilename, appStage, defaultLocation.Configuration);
            appConfigCabinet.AddBinder(xmlConfigBinder);
            xmlConfigBinder.Load();

            IConfigurationBinder configurationBinder = appConfigCabinet.GetBinder(configFilename);
            IList<ConfigurationItem> configItemList = configurationBinder.GetConfigurations();

            return configItemList;
        }

        //public static IList<ConfigurationItem> GetConfigList(String configFilename)
        public static String GetConfigList(String configFilename)
        {
            var envConfig = GetSystemConfig("Development-Stage") ?? new ConfigurationItem();
            String currentStage = envConfig.Value;
            if (String.IsNullOrEmpty(currentStage) || currentStage == AppStage.None)
                currentStage = AppStage.Development;
            //IList<ConfigurationItem> configList = GetConfigList(currentStage, configFilename);

            return currentStage;
        }
    }
}

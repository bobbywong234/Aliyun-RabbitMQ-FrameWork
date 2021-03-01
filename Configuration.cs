using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Options;
using System;
using System.IO;

namespace Infrastructure.Configs
{
    /// <summary>
    /// 配置文件 操作辅助类
    /// </summary>
    public static class ConfigHelper
    {
        public static IConfiguration Configuration  { get; set; }
        static ConfigHelper()
        {
            Configuration = new ConfigurationBuilder()
                .Add(new JsonConfigurationSource { Path = "appsettings.json", ReloadOnChange = true })
                .Build();
        }

        #region GetJsonConfig(获取Json配置文件)

        /// <summary>
        /// 获取Json配置文件
        /// </summary>
        /// <param name="configFileName">配置文件名。默认：appsettings.json</param>
        /// <param name="basePath">基路径</param>
        /// <returns></returns>
        public static IConfigurationRoot GetJsonConfig(string configFileName = "appsettings.json", string basePath = "")
        {
            basePath = string.IsNullOrWhiteSpace(basePath)
                ? Directory.GetCurrentDirectory()
                : Path.Combine(Directory.GetCurrentDirectory(), basePath);

            var configuration = new ConfigurationBuilder().SetBasePath(basePath)
                .AddJsonFile(configFileName, false, true)
                .Build();

            return configuration;
        }

        #endregion


        /// <summary>
        /// 根据配置文件和Key获得对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fileName">文件名称</param>
        /// <param name="key">节点Key</param>
        /// <returns></returns>
        public static T GetAppSettings<T>(string configFileName = "appsettings.json", string basePath = "", string key = "") where T : class, new()
        {
            //var baseDir = AppContext.BaseDirectory + "json/";
            //var currentClassDir = baseDir;
            basePath = string.IsNullOrWhiteSpace(basePath)
               ? Directory.GetCurrentDirectory()
               : Path.Combine(Directory.GetCurrentDirectory(), basePath);

            IConfiguration config = new ConfigurationBuilder()
                .SetBasePath(basePath)
                .Add(new JsonConfigurationSource { Path = configFileName, Optional = false, ReloadOnChange = true })
                .Build();
            var appconfig = new ServiceCollection().AddOptions()
                .Configure<T>(config.GetSection(key))
                .BuildServiceProvider()
                .GetService<IOptions<T>>()
                .Value;
            return appconfig;
        }


        #region GetXmlConfig(获取Xml配置文件)

        /// <summary>
        /// 获取Xml配置文件
        /// </summary>
        /// <param name="configFileName">配置文件名。默认：appsettings.xml</param>
        /// <param name="basePath">基路径</param>
        /// <returns></returns>
        public static IConfigurationRoot GetXmlConfig(string configFileName = "appsettings.xml", string basePath = "")
        {
            basePath = string.IsNullOrWhiteSpace(basePath)
                ? Directory.GetCurrentDirectory()
                : Path.Combine(Directory.GetCurrentDirectory(), basePath);

            var configuration = new ConfigurationBuilder().AddXmlFile(config =>
            {
                config.Path = configFileName;
                config.FileProvider = new PhysicalFileProvider(basePath);
            });

            return configuration.Build();
        }

        #endregion
    }
}

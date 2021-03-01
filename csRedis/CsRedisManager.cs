using CSRedis;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using System;
using System.Collections.Generic;
using System.Text;
using TM.Infrastructure.Configs;
using TM.Infrastructure.Json;

namespace Infrastructure.CSRedis
{
    /// <summary>
    /// CsRedis缓存服务
    /// 继承CsRedis官方提供的服务，这里只是对一些官方接口做了一下拓展，使用静态方式并初始化Redis服务器
    /// </summary>
    public abstract partial class CsRedisManager : RedisHelper
    {
        private static CSRedisClient _redisManager;
        static CsRedisManager()
        {
            _redisManager = new CSRedisClient(ConfigHelper.GetJsonConfig("appsettings.json").GetSection("RedisConnection").Value);      //Redis的连接字符串
            Initialization(_redisManager);
        }

        //https://www.cnblogs.com/additwujiahua/p/11479303.html

        /// <summary>
        /// 批处理模糊查询
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        internal static object BatchFuzzyQuery(string key)
        {
            try
            {
                var lau = @" local array = {" + key + "}" +
                          @" local t = { }" +
                          @" for iter, value in ipairs(array) do" +
                          @" local key = redis.call('keys', '*_'..value..'_*');" +
                          @" if #key>0 then" +
                          @" table.insert(t,key[1])" +
                          @" end " +
                          @" end " +
                          @" return  redis.call('mget', unpack(t))";
                return _redisManager.Eval(lau, "", "");
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// 批处理模糊滤波器
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        internal static object BatchFuzzyFilter(string key)
        {
            try
            {
                var lau = @" local array = {" + key + "}" +
                          @" local t = { }" +
                          @" for iter, value in ipairs(array) do" +
                          @" local key = redis.call('keys', value);" +
                          @" if #key>0 then" +
                          @" table.insert(t,key[1])" +
                          @" end " +
                          @" end " +
                          @" return  redis.call('mget', unpack(t))";
                return _redisManager.Eval(lau, "", "");
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// 批处理模糊
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        internal static object BatchFuzzy(string key)
        {
            try
            {
                var lau = @" local array = {" + key + "}" +
                          @" return  redis.call('mget', unpack(array))";
                return _redisManager.Eval(lau, "", "");
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// 处理模糊查询
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        internal static object FuzzyQuery(string key)
        {
            try
            {
                var lau = $" local keys = redis.call('keys', '{key}');" +
                          @" return  redis.call('mget', unpack(keys));";
                return _redisManager.Eval(lau, "", "");
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// 设置
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="val"></param>
        internal static void Set<T>(string key, T val)
        {
            try
            {
                _redisManager.Set(key, val);
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// 设置
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        internal static new T Get<T>(string key)
        {
            try
            {
                return _redisManager.Get<T>(key);
            }
            catch (Exception)
            {
                return default(T);
            }
        }



        /// <summary>
        /// TradeManageMessage 和 TradeManageMessage:MQ队列
        /// </summary>
        /// <returns></returns>
        public static bool EnQeenTradeManageMessage(string value)
        {
            try
            {
                //从头部插入 
                _redisManager.LPush("TradeManageMessage", value);
                _redisManager.LPush("TradeManageMessage:MQ", value);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// TradeManageMessage 和 TradeManageMessage:MQ队列
        /// </summary>
        /// <returns></returns>
        public static bool EnQeenTradeManageMessage<T>(T value)
        {
            try
            {
                //从头部插入 
                _redisManager.LPush("TradeManageMessage", value);
                _redisManager.LPush("TradeManageMessage:MQ", value);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// 将一个或多个值插入到列表头部
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool LPush(string key, string value)
        {
            try
            {
                //从头部插入 
                _redisManager.LPush(key, value);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// 移除并获取列表最后一个元素
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static new string RPop(string key)
        {
            try
            {
                //从尾部取值
                return _redisManager.RPop(key);
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// redis订阅模式
        /// 默认只订阅一个频道
        /// </summary>
        /// <param name="channel">频道</param>
        /// <param name="action"></param>
        public static void Subscribe(string channel, Action<string> action)
        {
            _redisManager.Subscribe((channel, msg => action(msg.Body)));
        }

        /// <summary>
        /// redis发布订阅
        /// </summary>
        /// <param name="channel">频道</param>
        /// <param name="action"></param>
        public static void Publish(string channel, Action<string> action)
        {
            _redisManager.Publish(channel, action.ToJson());
        }

        /// <summary>
        /// 对一个列表进行修剪，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static string[] DeQueenAll(string key)
        {
            string[] result = { };
            try
            {
                long len = _redisManager.LLen(key);
                //取出指定数量数据
                result = _redisManager.LRange(key, 0, len - 1);
                //删除指定数据
                bool res = _redisManager.LTrim(key, len, -1);
                return result;
            }
            catch (Exception)
            {
                return result;
            }
        }

        /// <summary>
        /// 将一个或多个值插入到列表头部
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool LPush<T>(string key, T value)
        {
            try
            {
                //从头部插入 
                long len = _redisManager.LPush(key, value);
                if (len > 0)
                    return true;
                else
                    return false;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// 移除并获取列表最后一个元素
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public static new T RPop<T>(string key)
        {
            try
            {
                //从尾部取值
                return _redisManager.RPop<T>(key);
            }
            catch (Exception)
            {
                return default(T);
            }
        }

        /// <summary>
        /// 设置hash值
        /// </summary>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool SetHash(string key, string field, string value)
        {
            try
            {
                return _redisManager.HSet(key, field, value);
            }
            catch (Exception)
            {
                return false;
            }
        }



        /// <summary>
        /// 根据表名，键名，获取hash值
        /// </summary>
        /// <param name="key">表名</param>
        /// <param name="field">键名</param>
        /// <returns></returns>
        public static string GetHash(string key, string field)
        {
            try
            {
                return _redisManager.HGet(key, field);
            }
            catch (Exception)
            {
                return null;
            }
        }


        /// <summary>
        /// 获取指定key中所有字段
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static Dictionary<string, string> GetHashAll(string key)
        {
            try
            {
                return _redisManager.HGetAll(key);
            }
            catch (Exception)
            {
                return new Dictionary<string, string>();
            }
        }


        /// <summary>
        /// 根据表名，键名，删除hash值
        /// </summary>
        /// <param name="key">表名</param>
        /// <param name="field">键名</param>
        /// <returns></returns>
        public static long DeleteHash(string key, string field)
        {
            try
            {
                return _redisManager.HDel(key, field);
            }
            catch (Exception)
            {
                return 0;
            }
        }

        /// <summary>
        /// 只有在 key 不存在时设置 key 的值
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static new bool SetNx(string key, object value)
        {
            try
            {
                return _redisManager.SetNx(key, value);
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// 为给定 key 设置过期时间
        /// </summary>
        /// <param name="key"></param>
        /// <param name="seconds"></param>
        /// <returns></returns>
        public static new bool Expire(string key, int seconds)
        {
            try
            {
                return _redisManager.Expire(key, seconds);
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// 用于在 key 存在时删除 key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static long Delete(params string[] key)
        {
            try
            {
                return _redisManager.Del(key);
            }
            catch (Exception)
            {
                return 0;
            }
        }

    }
}

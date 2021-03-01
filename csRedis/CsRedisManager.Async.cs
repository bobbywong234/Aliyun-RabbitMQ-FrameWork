
using CSRedis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Infrastructure.CSRedis
{
    /// <summary>
    /// 异步CsRedis缓存服务
    /// 继承CsRedis官方提供的服务，这里只是对一些官方接口做了一下拓展，使用静态方式并初始化Redis服务器   如果不需要可以并入ICache接口里面
    /// </summary>
    public abstract partial class CsRedisManager : RedisHelper
    {
        #region----Key/String----

        /// <summary>
        /// 检查给定 key 是否存在
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static new async Task<bool> ExistsAsync(string key)
        {
            try
            {
                return await _redisManager.ExistsAsync(key);
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// 获取指定 key 的值
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static new async Task<string> GetAsync(string key)
        {
            try
            {
                return await _redisManager.GetAsync(key);
            }
            catch (Exception)
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// 获取指定 key 的值
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static new async Task<T> GetAsync<T>(string key)
        {
            try
            {
                return await _redisManager.GetAsync<T>(key);
            }
            catch (Exception)
            {
                return default(T);
            }
        }

        /// <summary>
        /// 设置
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="val"></param>
        internal static async Task<bool> SetAsync<T>(string key, T val)
        {
            try
            {
                return await _redisManager.SetAsync(key, val);
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// 只有在 key 不存在时设置 key 的值
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static new async Task<bool> SetNxAsync(string key, object value)
        {
            try
            {
                return await _redisManager.SetNxAsync(key, value);
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// 设置指定 key 的值，所有写入参数object都支持string | byte[] | 数值 | 对象
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value">值</param>
        /// <param name="expireSeconds">过期(秒单位)</param>
        /// <param name="exists">0 Nx| 1 Xx</param>
        /// <returns></returns>
        public static new async Task<bool> SetAsync(string key, object value, int expireSeconds = -1, RedisExistence? exists = null)
        {
            try
            {
                return await _redisManager.SetAsync(key, value, expireSeconds, exists);
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        /// <summary>
        /// 为给定 key 设置过期时间
        /// </summary>
        /// <param name="key"></param>
        /// <param name="seconds">过期秒数</param>
        /// <returns></returns>
        public static new async Task<bool> ExpireAsync(string key, int seconds)
        {
            try
            {
                return await _redisManager.ExpireAsync(key, seconds);
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
        public static new async Task<long> DelAsync(params string[] key)
        {
            try
            {
                return await _redisManager.DelAsync(key);
            }
            catch (Exception)
            {
                return 0;
            }
        }

        /// <summary>
        /// 用于在 key 存在时删除 key（这个应该是写重复了，直播项目用DelAsync多，未知其他项目是否有用这个方法，就保留下来了）
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static async Task<long> DeleteAsync(params string[] key)
        {
            try
            {
                return await _redisManager.DelAsync(key);
            }
            catch (Exception)
            {
                return 0;
            }
        }


        /// <summary>
        /// 删除String类型的key（当键名与值匹配时才进行删除）
        /// </summary>
        /// <param name="key">成功返回1 |失败返回0</param>
        /// <returns></returns>
        public static async Task<long> DelStringKeyByValueAsync(string key, string value)
        {
            try
            {
                StringBuilder lau = new StringBuilder();
                lau.Append($" if redis.call('get', '{key}') == '{value}' ");
                lau.Append($"    then return redis.call('del', '{key}') ");
                lau.Append("  else return 0 end ");
                return (long)await _redisManager.EvalAsync(lau.ToString(), "", "");
            }
            catch (Exception)
            {
                return 0;
            }
        }

        /// <summary>
        /// 将 key 所储存的值加上给定的增量值（increment）
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static new async Task<long> IncrByAsync(string key, long value = 1)
        {
            try
            {
                return await _redisManager.IncrByAsync(key, value);
            }
            catch (Exception)
            {
                return 0;
            }
        }

        #endregion

        #region----Hash----

        /// <summary>
        /// 根据表名，键名，获取hash值
        /// </summary>
        /// <param name="key">表名</param>
        /// <param name="field">键名</param>
        /// <returns></returns>
        public static async Task<string> GetHashAsync(string key, string field)
        {
            try
            {
                return await _redisManager.HGetAsync(key, field);
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// 获取Hash中指定字段的值
        /// </summary>
        /// <param name="key">表名</param>
        /// <param name="field">键名</param>
        /// <returns></returns>
        public static async Task<T> GetHashAsync<T>(string key, string field)
        {
            try
            {
                return await _redisManager.HGetAsync<T>(key, field);
            }
            catch (Exception)
            {
                return default(T);
            }
        }

        /// <summary>
        /// 获取Hash中所有字段的值
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static async Task<Dictionary<string, string>> GetHashAllAsync(string key)
        {
            try
            {
                return await _redisManager.HGetAllAsync(key);
            }
            catch (Exception)
            {
                return new Dictionary<string, string>();
            }
        }

        /// <summary>
        /// 获取Hash中所有字段的值
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static async Task<Dictionary<string, T>> GetHashAllAsync<T>(string key)
        {
            try
            {
                return await _redisManager.HGetAllAsync<T>(key);
            }
            catch (Exception)
            {
                return new Dictionary<string, T>();
            }
        }

        /// <summary>
        /// 设置hash值
        /// 如果字段是哈希表中的一个新建字段，并且值设置成功，返回true。如果哈希表中域字段已经存在且旧值已被新值覆盖，返回false。
        /// </summary>
        /// <param name="key">键名</param>
        /// <param name="field">字段名</param>
        /// <param name="value">值</param>
        /// <returns></returns>
        public static async Task<bool> SetHashAsync(string key, string field, string value)
        {
            try
            {
                return await _redisManager.HSetAsync(key, field, value);
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// 设置hash值
        /// 只有在字段 field 不存在时，设置哈希表字段的值
        /// </summary>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static async Task<bool> SetHashNxAsync(string key, string field, string value)
        {
            try
            {
                return await _redisManager.HSetNxAsync(key, field, value);
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Hash 自增指定字段的数值
        /// </summary>
        /// <param name="key">键名</param>
        /// <param name="value">正加负减</param>
        /// <returns>成功 操作执行后的该字段的值 | 失败 返回null</returns>
        public static new async Task<long?> HIncrByAsync(string key, string field, long value = 1)
        {
            try
            {
                return await _redisManager.HIncrByAsync(key, field, value);
            }
            catch (Exception)
            {
                return new long?();
            }
        }

        /// <summary>
        /// 删除一个或多个哈希表字段
        /// </summary>
        /// <param name="key">表名</param>
        /// <param name="field">键名</param>
        /// <returns></returns>
        public static async Task<long> DeleteHashAsync(string key, params string[] field)
        {
            try
            {
                return await _redisManager.HDelAsync(key, field);
            }
            catch (Exception)
            {
                return 0;
            }
        }

        #endregion

        #region----List----

        /// <summary>
        /// 将一个或多个值插入到列表头部
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static async Task<bool> LPushAsync(string key, string value)
        {
            try
            {
                //从头部插入 
                await _redisManager.LPushAsync(key, value);
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
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static new async Task<bool> LPushAsync<T>(string key, params T[] value)
        {
            try
            {
                //从头部插入 
                long len = await _redisManager.LPushAsync(key, value);
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
        /// <param name="key"></param>
        /// <returns></returns>
        public static new async Task<string> RPopAsync(string key)
        {
            try
            {
                //从尾部取值
                return await _redisManager.RPopAsync(key);
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// 移除并获取列表最后一个元素
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public static new async Task<T> RPopAsync<T>(string key)
        {
            try
            {
                //从尾部取值
                return await _redisManager.RPopAsync<T>(key);
            }
            catch (Exception)
            {
                return default(T);
            }
        }

        /// <summary>
        /// 将旧列表source中的最后一个元素弹出，插入到新列表destination头部，并返回元素值给客户端。 
        /// 分布式Redis的需要对键名进行HashTag配置
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source">源key</param>
        /// <param name="destination">目标key</param>
        /// <returns></returns>
        public static new async Task<string> RPopLPushAsync(string source, string destination)
        {
            try
            {
                return await _redisManager.RPopLPushAsync(source, destination);
            }
            catch (Exception ex)
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// 将旧列表source中的最后一个元素弹出，插入到新列表destination头部，并返回元素值给客户端。 
        /// 分布式Redis的需要对键名进行HashTag配置
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source">源key</param>
        /// <param name="destination">目标key</param>
        /// <returns></returns>
        public static new async Task<T> RPopLPushAsync<T>(string source, string destination)
        {
            try
            {
                return await _redisManager.RPopLPushAsync<T>(source, destination);
            }
            catch (Exception ex)
            {
                return default(T);
            }
        }

        /// <summary>
        /// 获取列表长度
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static new async Task<long> LLenAsync(string key)
        {
            try
            {
                return await _redisManager.LLenAsync(key);
            }
            catch (Exception)
            {
                return 0;
            }
        }

        /// <summary>
        /// 通过索引获取列表中的元素 O(N) 
        /// 下标是从0开始，负数索则从列表尾部开始索引的元素
        /// 例如：-1 表示最后一个元素，-2 表示倒数第二个元素。（要留意入队顺序）
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static new async Task<string> LIndexAsync(string key, long index)
        {
            try
            {
                return await _redisManager.LIndexAsync(key, index);
            }
            catch (Exception ex)
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// 通过索引获取列表中的元素 O(N) 
        /// 下标是从0开始，负数索则从列表尾部开始索引的元素
        /// 例如：-1 表示最后一个元素，-2 表示倒数第二个元素。（要留意入队顺序）
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static new async Task<T> LIndexAsync<T>(string key, long index)
        {
            try
            {
                return await _redisManager.LIndexAsync<T>(key, index);
            }
            catch (Exception ex)
            {
                return default(T);
            }
        }

        /// <summary>
        /// 对一个列表进行修剪，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static async Task<string[]> DeQueenAllAsync(string key)
        {
            string[] result = { };
            try
            {
                long len = await _redisManager.LLenAsync(key);
                //取出指定数量数据
                result = await _redisManager.LRangeAsync(key, 0, len - 1);
                //删除指定数据
                bool res = await _redisManager.LTrimAsync(key, len, -1);
                return result;
            }
            catch (Exception)
            {
                return result;
            }
        }

        /// <summary>
        /// TradeManageMessage 和 TradeManageMessage:MQ队列
        /// </summary>
        /// <returns></returns>
        public static async Task<bool> EnQeenTradeManageMessageAsync(string value)
        {
            try
            {
                //从头部插入 
                await _redisManager.LPushAsync("TradeManageMessage", value);
                await _redisManager.LPushAsync("TradeManageMessage:MQ", value);
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
        public static async Task<bool> EnQeenTradeManageMessageAsync<T>(T value)
        {
            try
            {
                //从头部插入 
                await _redisManager.LPushAsync("TradeManageMessage", value);
                await _redisManager.LPushAsync("TradeManageMessage:MQ", value);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        #endregion

        #region----Set----

        #endregion

        #region----ZSet----

        #endregion

        #region----批量----

        /// <summary>
        /// 批处理模糊查询
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        internal static async Task<object> BatchFuzzyQueryAsync(string key)
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
                return await _redisManager.EvalAsync(lau, "", "");
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
        internal static async Task<object> BatchFuzzyFilterAsync(string key)
        {
            try
            {
                var lau = $" local array = '{key}'" +
                          @" local t = { }" +
                          @" for iter, value in ipairs(array) do" +
                          @" local key = redis.call('keys', value);" +
                          @" if #key>0 then" +
                          @" table.insert(t,key[1])" +
                          @" end " +
                          @" end " +
                          @" return  redis.call('mget', unpack(t))";
                return await _redisManager.EvalAsync(lau, "", "");
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
        internal static async Task<object> BatchFuzzyAsync(string key)
        {
            try
            {
                var lau = @" local array = {" + key + "}" +
                          @" return  redis.call('mget', unpack(array))";
                return await _redisManager.EvalAsync(lau, "", "");
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
        internal static async Task<object> FuzzyQueryAsync(string key)
        {
            try
            {
                var lau = $" local keys = redis.call('keys', '{key}');" +
                          @" return  redis.call('mget', unpack(keys));";
                return await _redisManager.EvalAsync(lau, "", "");
            }
            catch (Exception)
            {
                return null;
            }
        }

        #endregion

        #region ----锁-----
        /// <summary>开启分布式锁，若超时返回null</summary>
        /// <param name="name">锁名称</param>
        /// <param name="timeoutSeconds">超时（秒）</param>
        /// <param name="autoDelay">此锁为自动续锁机制，解锁的互斥访问信号由关中断请求发出</param>
        /// <returns></returns>
        public static CSRedisClientLock CsRedisLock(
            string name,
            int timeoutSeconds,
            bool autoDelay = true) {
            return _redisManager.Lock(name, timeoutSeconds, autoDelay);
        }
        #endregion
    }
}

using System;
using System.Threading.Tasks;
using TM.Infrastructure.Configs;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;
using System.Security.Cryptography;
using RabbitMQ.Client.Events;
using TM.Infrastructure.Helpers;
using TM.Infrastructure.CSRedis;

namespace TM.Infrastructure.RabbitMq {
    class AliyunMechanismFactory : IAuthMechanismFactory {
        /// <summary>
        /// The name of the authentication mechanism, as negotiated on the wire.
        /// </summary>
        public string Name {
            get { return "PLAIN"; }
        }

        private readonly string UserName;
        private readonly string Password;
        private readonly string HostName;

        public AliyunMechanismFactory(string userName, string password, string hostName) {
            UserName = userName;
            Password = password;
            HostName = hostName;
        }

        /// <summary>
        /// Return a new authentication mechanism implementation.
        /// </summary>
        IAuthMechanism IAuthMechanismFactory.GetInstance() {
            return new AliyunMechanism(UserName, Password, HostName);
        }
    }

    class AliyunMechanism : IAuthMechanism {
        private static readonly int FROM_USER = 0;
        private static readonly string COLON = ":";
        private static readonly DateTime EPOCH_START = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// base 64 convertor
        /// </summary>
        /// <param name="ak"></param>
        /// <param name="resourceOwnerId"></param>
        /// <returns></returns>
        private static string Base64Parser(string ak, string resourceOwnerId) {
            StringBuilder data = new StringBuilder(64);
            data.Append(FROM_USER).Append(COLON).Append(resourceOwnerId).Append(COLON).Append(ak);
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(data.ToString()));
        }

        /// <summary>
        /// password encryption(sha1) for aliyun
        /// </summary>
        /// <param name="sk">mq password</param>
        /// <returns></returns>
        private static string PasswordEncryption(string sk) {
            TimeSpan ts = DateTime.UtcNow - EPOCH_START;
            long timestamp = Convert.ToInt64(ts.TotalMilliseconds);

            KeyedHashAlgorithm algorithm = KeyedHashAlgorithm.Create("HMACSHA1");
            if (null == algorithm) {
                throw new InvalidOperationException("HMACSHA1 not exist!");
            }

            try {
                algorithm.Key = Encoding.UTF8.GetBytes(timestamp.ToString());
                byte[] bytes = algorithm.ComputeHash(Encoding.UTF8.GetBytes(sk));
                string signature = byteArrayToUpperString(bytes);

                StringBuilder data = new StringBuilder(64);
                data.Append(signature).Append(COLON).Append(timestamp);
                return Convert.ToBase64String(Encoding.UTF8.GetBytes(data.ToString()));
            }
            finally {
                algorithm.Clear();
            }
        }

        /// <summary>
        /// bytes string formater
        /// </summary>
        /// <param name="bytes"></param>
        /// <returns></returns>
        private static string byteArrayToUpperString(byte[] bytes) {
            if (bytes == null) {
                throw new ArgumentNullException("bytes array null");
            }
            StringBuilder hex = new StringBuilder(bytes.Length * 2);
            foreach (byte b in bytes) {
                hex.AppendFormat("{0:x2}", b);
            }
            return hex.ToString().ToUpper();
        }

        /// <summary>
        /// encrypted user name
        /// </summary>
        private readonly string AuthUserName;
        /// <summary>
        /// encrypted password
        /// </summary>
        private readonly string AuthPassword;


        /// <summary>
        /// Aliyun Authentication Construction
        /// </summary>
        /// <param name="userName"></param>
        /// <param name="password"></param>
        /// <param name="hostName"></param>
        public AliyunMechanism(string userName, string password, string hostName) {
            AuthUserName = GetUserName(userName, hostName);
            AuthPassword = PasswordEncryption(password);
        }

        /// <summary>
        /// IAuthMechanism implementation
        /// </summary>
        /// <param name="challenge"></param>
        /// <param name="factory"></param>
        /// <returns></returns>
        public byte[] handleChallenge(byte[] challenge, IConnectionFactory factory) {
            if (factory is ConnectionFactory) {
                return Encoding.UTF8.GetBytes("\0" + AuthUserName + "\0" + AuthPassword);
            }
            else {
                throw new InvalidCastException("need ConnectionFactory");
            }
        }

        /// <summary>
        /// combin hostname with username
        /// </summary>
        /// <param name="userNmae"></param>
        /// <param name="hostName"></param>
        /// <returns></returns>
        private static string GetUserName(string userNmae, string hostName) {
            string ownerResourceId;
            try {
                string[] sArray = hostName.Split('.');
                ownerResourceId = sArray[0];
            }
            catch {
                throw new InvalidProgramException("hostName invalid");
            }
            return Base64Parser(userNmae, ownerResourceId);
        }
    }

    /// <summary>
    /// 如需多线程访问MQ,线程最高只能生成100（与对应得最大渠道数一致）
    /// </summary>
    public static class RabbitMQFramWork {

        /// <summary>
        /// 委托型消费队列后续自定义方法
        /// </summary>
        /// <returns></returns>
        public delegate Task PostConsumptionTask(string input);
        /// <summary>
        /// 消费队列失败位图存储，redis hashmap 键值前缀
        /// </summary>
        public static readonly string MQFailureConsumptionRedisKey = "Consumer:Failure:";

        /// <summary>
        /// MQ 连接工厂
        /// </summary>
        private static ConnectionFactory MQConnection = new ConnectionFactory() {
            UserName = ConfigHelper.Configuration["MQConnection:UserName"], //实例用户名
            VirtualHost = ConfigHelper.Configuration["MQConnection:VirtualHost"], //实例虚拟环境服务器名
            HostName = ConfigHelper.Configuration["MQConnection:HostName"],//endpoint 服务器名
            Password = ConfigHelper.Configuration["MQConnection:Password"],//实例密码
            Port = Convert.ToInt32(ConfigHelper.Configuration["MQConnection:Port"]),//实例端口
            NetworkRecoveryInterval = new TimeSpan(hours: 0, minutes: 0, seconds: 3),//重连间隔时间
            RequestedConnectionTimeout = new TimeSpan(hours: 0, minutes: 20, seconds: 0),//访问超时时间
            RequestedChannelMax = 100,//最大渠道数
            RequestedFrameMax = 65535,//DataFrame数据包最大字节数
            DispatchConsumersAsync = true,//队列异步消费启用
            RequestedHeartbeat = new TimeSpan(hours: 0, minutes: 0, seconds: 12),//服务器握手时间间隔
            ContinuationTimeout = new TimeSpan(hours: 0, minutes: 20, seconds: 0),//队列生成等待时间
            ConsumerDispatchConcurrency = 8,//消费转接站并发数
            AuthMechanisms = new List<IAuthMechanismFactory>()
            {
                new AliyunMechanismFactory
                (
                   userName: ConfigHelper.Configuration["MQConnection:UserName"],
                   password: ConfigHelper.Configuration["MQConnection:Password"],
                   hostName: ConfigHelper.Configuration["MQConnection:HostName"]
                )
            } //认证列表

        };
        /// <summary>
        /// 散播中转站名
        /// </summary>
        private static readonly string FanOut = "amq.fanout";
        /// <summary>
        /// 话题中转站名
        /// </summary>
        private static readonly string Topic = "amq.topic";
        /// <summary>
        /// 直传中转站名
        /// </summary>
        private static readonly string Dircet = "amq.direct";

        #region ----Message Publish Method----
        /// <summary>
        /// 直传发送
        /// </summary>
        /// <param name="queueName">队列名</param>
        /// <param name="parameters">数据消息</param>
        /// <param name="IsPersistent">消息是否保留 1-不耐久，2-耐久</param>
        /// <param name="Expiration">耐久信息超时时间,单位ms</param>
        /// <param name="routingKeys">导向键值，即消息经过中转站后所指向的多个队列的索引值</param>
        /// <returns>-2->实例熔断,-1->渠道熔断,0->发送超时, 1->发送完毕</returns>
        public static async Task<short> DirectPublishAsync(string queueName, string parameters, byte IsPersistent, int Expiration = 3600, HashSet<string> routingKeys = null) {
            short flag = 1;
            var messageBody = Encoding.UTF8.GetBytes(parameters);
            using (var connector = MQConnection.CreateConnection()) {
                if (connector.IsOpen)
                    using (var channel = connector.CreateModel()) {
                        if (channel.IsOpen) {
                            var props = channel.CreateBasicProperties();
                            props.DeliveryMode = IsPersistent;
                            props.Expiration = Expiration.ToString();

                            if (routingKeys == null) {
                                channel.QueueDeclare(queue: queueName, durable: true, exclusive: false, autoDelete: true);
                                channel.QueueBind(queue: queueName, exchange: Dircet, routingKey: Dircet);
                                channel.BasicPublish(exchange: Dircet, routingKey: Dircet, body: messageBody);
                                await Task.Yield();
                                channel.ConfirmSelect();
                                try { channel.WaitForConfirmsOrDie(new TimeSpan(hours: 0, minutes: 19, seconds: 59)); }
                                catch { flag = 0; }
                            }
                            else
                                foreach (var item in routingKeys) {
                                    channel.QueueDeclare(queue: $"{queueName}.{item}", durable: true, exclusive: false, autoDelete: true);
                                    channel.QueueBind(queue: $"{queueName}.{item}", exchange: Dircet, routingKey: Dircet);
                                    channel.BasicPublish(exchange: Dircet, routingKey: Dircet, body: messageBody);
                                    await Task.Yield();
                                    channel.ConfirmSelect();
                                    try { channel.WaitForConfirmsOrDie(new TimeSpan(hours: 0, minutes: 19, seconds: 59)); break; }
                                    catch { flag = 0; }
                                }

                            channel.Close();
                        }
                        else
                            flag = -1;

                    }
                else
                    flag = -2;
            }
            return flag;
        }

        /// <summary>
        /// 散播发送
        /// </summary>
        /// <param name="queueNames">队列名</param>
        /// <param name="parameters">数据消息</param>
        /// <param name="IsPersistent">消息是否保留 1-不耐久，2-耐久</param>
        /// <param name="Expiration">耐久信息超时时间,单位ms</param>
        /// <returns>-2->实例熔断,-1->渠道熔断,0->发送超时, 1->发送完毕</returns>
        public static async Task<short> FanOutPublishAsync(List<string> queueNames, string parameters, byte IsPersistent, int Expiration = 3600) {
            short flag = 1;
            var messageBody = Encoding.UTF8.GetBytes(parameters);
            using (var connector = MQConnection.CreateConnection()) {
                if (connector.IsOpen)
                    using (var channel = connector.CreateModel()) {
                        if (channel.IsOpen) {
                            var props = channel.CreateBasicProperties();
                            props.DeliveryMode = IsPersistent;
                            props.Expiration = Expiration.ToString();

                            queueNames.ForEach(item => {
                                channel.QueueDeclare(queue: item, durable: true, exclusive: false, autoDelete: true);
                                channel.QueueBind(queue: item, exchange: FanOut, routingKey: FanOut);
                            });
                            channel.BasicPublish(exchange: FanOut, routingKey: FanOut, body: messageBody);
                            await Task.Yield();
                            channel.ConfirmSelect();
                            try { channel.WaitForConfirmsOrDie(new TimeSpan(hours: 0, minutes: 19, seconds: 59)); }
                            catch { flag = 0; }
                            channel.Close();
                        }
                        else
                            flag = -1;

                    }
                else
                    flag = -2;
            }
            return flag;
        }

        /// <summary>
        /// 话题发送(kevaluepairs列表结构)
        /// </summary>
        /// <param name="topics"></param>
        /// <param name="parameters"></param>
        /// <param name="IsPersistent">消息是否保留 1-不耐久，2-耐久</param>
        /// <param name="Expiration">耐久信息超时时间,单位ms</param>
        /// <returns>-2->实例熔断,-1->渠道熔断,0->发送超时, 1->发送完毕</returns>
        public static async Task<short> TopicPublishAsync(List<KeyValuePair<string, string>> topics, byte IsPersistent, int Expiration = 3600) {
            short flag = 1;

            using (var connector = MQConnection.CreateConnection()) {
                if (connector.IsOpen)
                    using (var channel = connector.CreateModel()) {
                        if (channel.IsOpen) {
                            var props = channel.CreateBasicProperties();
                            props.DeliveryMode = IsPersistent;
                            props.Expiration = Expiration.ToString();

                            foreach (var item in topics) {
                                var messageBody = Encoding.UTF8.GetBytes(item.Value);
                                channel.QueueDeclare(queue: item.Key, durable: true, exclusive: false, autoDelete: true);
                                channel.QueueBind(queue: item.Key, exchange: Topic, routingKey: $"{item.Key}.#");
                                channel.BasicPublish(exchange: Topic, routingKey: $"{item.Key}.{item.Value}", body: messageBody);
                                await Task.Yield();
                            }
                            channel.ConfirmSelect();
                            try { channel.WaitForConfirmsOrDie(new TimeSpan(hours: 0, minutes: 19, seconds: 59)); }
                            catch { flag = 0; }
                            channel.Close();
                        }
                        else
                            flag = -1;

                    }
                else
                    flag = -2;
            }
            return flag;
        }
        #endregion

        #region ----Broker Consumer----
        /// <summary>
        /// 委托事件消费队列，(如在hash表中检查出有失败得渠道id,则消费超时)
        /// </summary>
        /// <param name="queueName">队列名</param>
        /// <param name="postTask">消费后执行事务</param>
        /// <param name="sleepTime">等待时间，单位(微秒),由PV信号量控制,建议设为1ms</param>
        /// <returns></returns>
        public static async Task<string> ConsumerAsyncDelegation(string queueName, PostConsumptionTask postTask, int sleepTime) {
            using (var connector = MQConnection.CreateConnection()) {
                if (connector.IsOpen)
                    using (var channel = connector.CreateModel()) {
                        if (channel.IsOpen) {
                            var consumer = new AsyncEventingBasicConsumer(channel);

                            consumer.Received += async (ch, response) => {
                                using (var redisLock = CsRedisManager.CsRedisLock(queueName, 20 * 60)) {
                                    var data = response.Body.ToArray();
                                    var value = Encoding.UTF8.GetString(data);
                                    try {
                                        await postTask(value);
                                        channel.BasicAck(response.DeliveryTag, false);
                                        await consumer.HandleBasicCancel(response.ConsumerTag);
                                    }
                                    catch {
                                        await CsRedisManager.HSetAsync($"{MQFailureConsumptionRedisKey}{queueName}", response.DeliveryTag.ToString(), value);
                                        await consumer.HandleBasicCancel(response.ConsumerTag);
                                    }
                                    redisLock.Unlock();
                                }
                            };
                            channel.BasicConsume(queueName, false, consumer);

                            //PV Signaling
                            var signal = true;
                            while (!consumer.IsRunning && signal) {
                                System.Threading.Thread.Sleep(sleepTime);
                                while (consumer.IsRunning) {
                                    signal = false;
                                }
                            }

                            await Task.Yield();
                            return "success";
                        }
                        else
                            return "";
                    }
                else
                    return "";
            }
        }

        /// <summary>
        /// 同步线程消费队列
        /// </summary>
        /// <param name="queueName">队列名</param>
        /// <returns>为空为消费未成功</returns>
        public static async Task<string> ConsumerAsync(string queueName) {
            using (var connector = MQConnection.CreateConnection()) {
                if (connector.IsOpen)
                    using (var channel = connector.CreateModel()) {
                        if (channel.IsOpen) {
                            var get = channel.BasicGet(queueName, true).Body.ToArray();
                            await Task.Yield();
                            if (channel.MessageCount(queueName).Equals(0)) {
                                channel.QueueDeleteNoWait(queueName);
                                var signal = 0;
                                while (await IsQueueExist(queueName) && signal < 4) {
                                    System.Threading.Thread.Sleep(50);
                                    signal += 1;
                                }
                            }
                            return Encoding.UTF8.GetString(get);
                        }
                        else
                            return "";
                    }
                else
                    return "";
            }
        }
        #endregion

        #region ----Queue Management----
        /// <summary>
        /// 检查是否有队列
        /// </summary>
        /// <param name="queueName"></param>
        /// <returns></returns>
        public async static Task<bool> IsQueueExist(string queueName) {
            var flag = false;
            using (var connector = MQConnection.CreateConnection()) {
                if (connector.IsOpen)
                    using (var channel = connector.CreateModel()) {
                        if (channel.IsOpen) {
                            try {
                                var t = channel.QueueDeclarePassive(queueName);
                                await Task.Yield();
                                flag = true;
                            }
                            catch {

                            }
                            channel.Close();
                        }

                    }
            }
            return flag;
        }

        #endregion
    }
}

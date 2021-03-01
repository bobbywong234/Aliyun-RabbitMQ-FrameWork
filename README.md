# Aliyun-RabbitMQ-FrameWork

Self-Developed RabbitMQ Framework includs:<br /> 
consumer recevied event delegation, consumer poll, topic publish, direct publish, and fanout publish.<br /><br />
The Aliyun Authentication Code would also be programmed in this example<br />
Configuration.cs is part of code which checks application path for accessing MQ configuration string variable <br /><br />
All the configuration setup file was in the MQConfiguration.cs<br /><br />
While it also used CsRedis as a locking tool, to record failure of MQ consume and apply distributed redis lock<br />
This part of Code is in the csRedis file folder<br /><br />


Producer（生产者）

  // 第1句：功能定位
  封装 Kafka 同步生产者，管理多个 Kafka 连接，配置高可靠性参数（等待所有副本确认）。

  // 第2句：核心流程
  InitKafkaProducer 初始化 → 存入 map → GetProducer 获取实例 → 发送消息。

  Consumer（消费者）

  // 第1句：功能定位
  封装 Kafka 消费者组，支持订阅多个主题，从最早消息开始消费，通过回调函数处理消息。

  // 第2句：核心流程
  NewConsumerGroup 创建 → Start 启动消费循环 → 收到消息 → 调用用户传入的 MessageHandleFunc → 标记消息已处理。
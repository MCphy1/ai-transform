cos_upload.go：生成腾讯云 COS 的临时密钥和预签名 URL，让用户能直接上传文件到云存储。

transform.go：接收视频翻译请求，将任务记录存入数据库并推送到 Kafka 队列进行异步处理。
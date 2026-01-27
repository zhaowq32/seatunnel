```yaml
source:
  type: OceanBase-CDC
  
  # 基础连接配置
  jdbc-url: "jdbc:oceanbase://localhost:2883/test"
  username: "user@tenant_name"
  password: "password"
  tenant-name: "tenant_name"
  
  # 系统租户支持（可选）
  sys-username: "root@sys"
  sys-password: "sys_password"
  
  # LogProxy 配置
  logproxy.host: "localhost"
  logproxy.port: 2983
  
  # 集群配置（二选一）
  config-url: "http://config-server:8080"
  # 或
  rs-list: "127.0.0.1:2882:2881"
  
  # 表过滤
  table-list: ["db1.table1", "db2.table2"]
  # 或单表
  table-name: "db.table"
  
  # 时区配置（字符串格式）
  server-time-zone: "+08:00"
  
  # 连接配置
  connect.timeout.ms: 30000
  max-reconnect-times: 10
  reconnect-interval.ms: 5000
  
  # 高级配置
  working-mode: "storage"  # memory 或 storage
  start-timestamp: 1234567890  # 可选
  
  # 性能配置
  split-size: 10000
  batch-size: 1024
  exactly-once: false
  
  # CDC 模式
  startup-mode: "initial"  # initial, latest, timestamp
```

type:BEGIN
record_id:360287970189639680
db:null
tb:null
serverId:null
checkpoint:258344@1769079457
primary_value:
unique_keys:



type:UPDATE
record_id:360287970189639680
db:test.test
tb:billing_charge_user_test
serverId:null
checkpoint:258344@1769079457
primary_value:USER_ID
unique_keys:

Field name: USER_ID
Field type: 8
Field length: 1
Field notNull: true
Field value: 3
Field name: USER_ID
Field type: 8
Field length: 1
Field notNull: true
Field value: 3
Field name: CYCLE_ID
Field type: 15
Field length: 1
Field notNull: true
Field value: 4
Field name: CYCLE_ID
Field type: 15
Field length: 1
Field notNull: true
Field value: 4
Field name: ACCOUNT_ID
Field type: 8
Field length: 1
Field notNull: true
Field value: 5
Field name: ACCOUNT_ID
Field type: 8
Field length: 1
Field notNull: true
Field value: 1
Field name: SVC_TYPE
Field type: 8
Field length: 1
Field notNull: true
Field value: 4
Field name: SVC_TYPE
Field type: 8
Field length: 1
Field notNull: true
Field value: 4
Field name: SUBJECT_ID
Field type: 8
Field length: 1
Field notNull: true
Field value: 7
Field name: SUBJECT_ID
Field type: 8
Field length: 1
Field notNull: true
Field value: 7
Field name: COMPONENT_ID
Field type: 15
Field length: 1
Field notNull: true
Field value: 8
Field name: COMPONENT_ID
Field type: 15
Field length: 1
Field notNull: true
Field value: 8
Field name: TOTAL_FEE
Field type: 8
Field length: 1
Field notNull: true
Field value: 9
Field name: TOTAL_FEE
Field type: 8
Field length: 1
Field notNull: true
Field value: 9


type:COMMIT
record_id:360287970189639680
db:null
tb:null
serverId:null
checkpoint:258344@1769079457
primary_value:
unique_keys:




# mongo 导出导入
> 工具下载地址(注意要与mongodb版本对应): https://www.mongodb.com/try/download/database-tools

在根目录下创建 config.ini:
类似:
```ini
[global]
databases=message,code,db2ss

[source]
db_host=16.15.64.56
db_port=27017
db_user=***
db_pass=***

[target]
db_host=10.96.104.15
db_port=27017
db_user=***
db_pass=***
```
然后运行:
```python
python main.py
```# mongo-processor
# mongo-processor

# mongo 导出导入
> 工具下载地址(注意要与mongodb版本对应): https://www.mongodb.com/try/download/database-tools
> 版本对应: https://www.mongodb.com/docs/database-tools/mongodump/

下载地址：
https://fastdl.mongodb.org/tools/db/mongodb-database-tools-macos-arm64-100.9.0.zip
其他版本可以类似修改版本号和平台标志解决下载地址找不到的问题

在根目录下创建 config.ini:
类似:
```ini
[global]
databases=code2,ddd,xxx

[source]
host=161.189.137.33
port=27171
username=business
password=xxx

[target]
host=118.145.81.22
port=3717
username=root
password=xxx
```
然后运行:
```Python
python main.py
```

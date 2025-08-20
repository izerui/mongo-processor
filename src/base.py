import os
import platform
from subprocess import Popen, PIPE, STDOUT
from typing import Optional, Dict, Any


class Shell(object):

    def _exe_command(self, command):
        """
        执行 shell 命令并实时打印输出
        :param command: shell 命令
        :return: process, exitcode
        """
        print(command)
        process = Popen(command, stdout=PIPE, stderr=STDOUT, shell=True)
        with process.stdout:
            for line in iter(process.stdout.readline, b''):
                try:
                    print(line.decode().strip())
                except:
                    print(str(line))
        exitcode = process.wait()
        if exitcode != 0:
            raise BaseException('命令执行失败')
        return process, exitcode


class Mongo:
    __slots__ = ['host', 'port', 'username', 'password']

    def __init__(self, db_host, db_port, db_user, db_pass):
        self.host = db_host
        self.port = db_port
        self.username = db_user
        self.password = db_pass


class ShardConfig:
    """分片配置类"""

    def __init__(self):
        self.min_documents_for_shard = 1000000  # 分片最小文档数
        self.default_shard_count = 4  # 默认分片数
        self.max_shard_count = 16  # 最大分片数




class ObjectIdRange:
    """ObjectId范围类"""

    def __init__(self, start_id=None, end_id=None):
        self.start_id = start_id
        self.end_id = end_id

    def to_query(self) -> Optional[Dict[str, Any]]:
        """转换为MongoDB查询条件"""
        if not self.start_id and not self.end_id:
            return None

        query = {}
        if self.start_id and self.end_id:
            query['_id'] = {'$gte': self.start_id, '$lt': self.end_id}
        elif self.start_id:
            query['_id'] = {'$gte': self.start_id}
        elif self.end_id:
            query['_id'] = {'$lt': self.end_id}

        return query

    def __str__(self):
        return f"ObjectIdRange({self.start_id}, {self.end_id})"


# 获取当前脚本目录（src目录）
script_dir = os.path.dirname(os.path.abspath(__file__))
# 获取项目根目录（包含mongodb-database-tools的目录）
base_dir = os.path.dirname(script_dir)

if platform.system() == 'Windows':
    mongodump_exe = os.path.join(base_dir, 'mongodb-database-tools', 'windows-x86_64-100.13.0', 'mongodump.exe')
    mongorestore_exe = os.path.join(base_dir, 'mongodb-database-tools', 'windows-x86_64-100.13.0', 'mongorestore.exe')
elif platform.system() == 'Linux':
    mongodump_exe = os.path.join(base_dir, 'mongodb-database-tools', 'rhel93-x86_64-100.13.0', 'mongodump')
    mongorestore_exe = os.path.join(base_dir, 'mongodb-database-tools', 'rhel93-x86_64-100.13.0', 'mongorestore')
elif platform.system() == 'Darwin':
    mongodump_exe = os.path.join(base_dir, 'mongodb-database-tools', 'macos-arm64-100.13.0', 'mongodump')
    mongorestore_exe = os.path.join(base_dir, 'mongodb-database-tools', 'macos-arm64-100.13.0', 'mongorestore')

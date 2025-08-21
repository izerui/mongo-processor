import os
import platform
import subprocess
import sys
import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any, List

from pymongo import IndexModel
from pymongo import MongoClient
from pymongo.errors import OperationFailure, DuplicateKeyError


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


class MyMongo(object):

    def __init__(self, mongo: Mongo):
        self.mongo = mongo
        self._init_client()
        self._init_mongo_exe()

    def _init_client(self):
        """初始化MongoDB客户端"""
        try:
            if self.mongo.username and self.mongo.password:
                uri = f"mongodb://{self.mongo.username}:{self.mongo.password}@{self.mongo.host}:{self.mongo.port}/admin"
            else:
                uri = f"mongodb://{self.mongo.host}:{self.mongo.port}/"

            self.client = MongoClient(uri)
            return True
        except Exception as e:
            raise BaseException(f"❌ MongoDB连接失败: {e}")

    def get_database_indexes(self, database_name: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        获取指定数据库的所有集合的索引信息

        Args:
            database_name: 数据库名称

        Returns:
            字典，键为集合名，值为索引列表
        """
        indexes_info = {}

        try:
            db = self.client[database_name]

            # 获取数据库中的所有集合
            collections = db.list_collection_names()

            for collection_name in collections:
                try:
                    # 获取集合的索引信息
                    indexes = db[collection_name].list_indexes()
                    indexes_info[collection_name] = []

                    for index in indexes:
                        # 跳过默认的_id索引
                        if index['name'] == '_id_':
                            continue

                        # 构建索引信息
                        index_info = {
                            'name': index['name'],
                            'key': index['key'],
                            'unique': index.get('unique', False),
                            'sparse': index.get('sparse', False),
                            'background': True,  # 强制后台创建
                            'expireAfterSeconds': index.get('expireAfterSeconds'),
                            'partialFilterExpression': index.get('partialFilterExpression'),
                            'collation': index.get('collation')
                        }

                        # 移除None值
                        index_info = {k: v for k, v in index_info.items() if v is not None}
                        indexes_info[collection_name].append(index_info)

                except OperationFailure as e:
                    print(f"获取集合 {collection_name} 的索引失败: {e}")
                    continue

        except Exception as e:
            print(f"获取数据库 {database_name} 的索引信息失败: {e}")

        return indexes_info

    def create_indexes_on_target(self, database_name: str, indexes_info: Dict[str, List[Dict[str, Any]]]) -> Dict[
        str, bool]:
        """
        在目标数据库上创建索引

        Args:
            database_name: 数据库名称
            indexes_info: 索引信息字典

        Returns:
            字典，键为集合名，值为是否成功
        """
        results = {}
        print(f"\n📂 数据库 {database_name} 开始创建索引...")

        for collection_name, indexes in indexes_info.items():
            try:
                target_db = self.client[database_name]
                target_collection = target_db[collection_name]

                if not indexes:
                    print(f"   📄 集合 {collection_name}: 无索引需要创建")
                    results[collection_name] = True
                    continue

                print(f"   📄 集合 {collection_name}: 准备创建 {len(indexes)} 个索引")

                # 准备索引模型
                index_models = []
                for index_info in indexes:
                    # 提取索引键
                    keys = list(index_info['key'].items())

                    # 创建索引选项
                    options = {
                        'name': index_info['name'],
                        'background': True,  # 强制后台创建
                    }

                    # 添加其他选项
                    if 'unique' in index_info:
                        options['unique'] = index_info['unique']
                    if 'sparse' in index_info:
                        options['sparse'] = index_info['sparse']
                    if 'expireAfterSeconds' in index_info:
                        options['expireAfterSeconds'] = index_info['expireAfterSeconds']
                    if 'partialFilterExpression' in index_info:
                        options['partialFilterExpression'] = index_info['partialFilterExpression']
                    if 'collation' in index_info:
                        options['collation'] = index_info['collation']

                    index_model = IndexModel(keys, **options)
                    index_models.append(index_model)

                # 批量创建索引
                if index_models:
                    created_names = [idx['name'] for idx in indexes]
                    target_collection.create_indexes(index_models)

                    # 打印创建的索引详情
                    for idx in indexes:
                        index_desc = f"{idx['name']}: {list(idx['key'].items())}"
                        if idx.get('unique'):
                            index_desc += " [唯一]"
                        if idx.get('sparse'):
                            index_desc += " [稀疏]"
                        print(f"      ✅ 创建索引 {index_desc}")

                results[collection_name] = True
                print(f"成功在集合 {collection_name} 上创建 {len(indexes)} 个索引")

            except DuplicateKeyError as e:
                print(f"      ⚠️  集合 {collection_name} 索引已存在，跳过创建")
                results[collection_name] = True  # 视为成功，因为索引已存在
            except OperationFailure as e:
                print(f"      ❌ 集合 {collection_name} 索引创建失败: {e}")
                results[collection_name] = False
            except Exception as e:
                print(f"      ❌ 集合 {collection_name} 发生未知错误: {e}")
                results[collection_name] = False

        return results

    def _init_mongo_exe(self):
        """初始化MongoDB执行命令"""
        # 获取当前脚本目录（src目录）
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # 获取项目根目录（包含mongodb-database-tools的目录）
        base_dir = os.path.dirname(script_dir)

        if platform.system() == 'Windows':
            self.mongodump_exe = os.path.join(base_dir, 'mongodb-database-tools', 'windows-x86_64-100.13.0',
                                              'mongodump.exe')
            self.mongorestore_exe = os.path.join(base_dir, 'mongodb-database-tools', 'windows-x86_64-100.13.0',
                                                 'mongorestore.exe')
        elif platform.system() == 'Linux':
            self.mongodump_exe = os.path.join(base_dir, 'mongodb-database-tools', 'rhel93-x86_64-100.13.0', 'mongodump')
            self.mongorestore_exe = os.path.join(base_dir, 'mongodb-database-tools', 'rhel93-x86_64-100.13.0',
                                                 'mongorestore')
        elif platform.system() == 'Darwin':
            self.mongodump_exe = os.path.join(base_dir, 'mongodb-database-tools', 'macos-arm64-100.13.0', 'mongodump')
            self.mongorestore_exe = os.path.join(base_dir, 'mongodb-database-tools', 'macos-arm64-100.13.0',
                                                 'mongorestore')
        raise BaseException(f"❌ 不支持的操作系统,无法初始化MongoDB执行命令")

    def get_database_collections(self, database: str) -> List[str]:
        """获取数据库中的所有collection名称"""
        try:
            db = self.client[database]
            # 获取所有collection名称，排除系统collection
            collections = [name for name in db.list_collection_names()
                           if not name.startswith('system.')]
            return collections
        except Exception as e:
            print(f"❌ 获取数据库 {database} collection列表失败: {e}")
            return []

    def get_collection_counts_fast(self, database: str) -> dict:
        """使用快速统计信息获取集合文档数量"""
        try:
            if not self._connect():
                return {}

            db = self.client[database]
            collection_stats = {}

            # 使用listCollections获取所有集合
            collections = db.list_collections()
            for collection_info in collections:
                collection_name = collection_info['name']
                if collection_name.startswith('system.'):
                    continue

                try:
                    # 使用stats()获取快速统计信息
                    stats = db.command('collStats', collection_name)
                    count = stats.get('count', 0)
                    collection_stats[collection_name] = count
                    print(f"📊 {database}.{collection_name}: {count:,}条文档")
                except Exception as e:
                    print(f"⚠️ 获取{database}.{collection_name}统计失败: {e}")
                    collection_stats[collection_name] = 0

            return collection_stats

        except Exception as e:
            print(f"❌ 获取数据库 {database} 集合统计信息失败: {e}")
            return {}

    def __del__(self):
        """断开MongoDB连接"""
        if self.client:
            self.client.close()

    def exe_command(self, command, timeout=None, debug=True, show_timestamp=True):
        """
        执行命令并实时输出日志，支持时间戳和进度显示
        :param command: shell 命令
        :param timeout: 超时时间（秒），None表示无超时限制
        :param debug: 是否显示调试信息
        :param show_timestamp: 是否在每行输出前添加时间戳
        :return: result, exitcode
        """
        start_time = time.time()
        start_datetime = datetime.now()

        print(f"🚀 开始执行: \n{command}\n")

        def kill_process(p):
            """超时后杀死进程"""
            try:
                p.kill()
                print(f"⏰ [{datetime.now().strftime('%H:%M:%S')}] 进程已被强制终止")
            except:
                pass

        try:
            # 使用Popen实现实时输出，合并stdout和stderr
            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # 合并错误输出到标准输出
                text=True,
                bufsize=1,
                universal_newlines=True
            )

            # 设置超时定时器
            timer = None
            if timeout is not None and timeout > 0:
                timer = threading.Timer(timeout, kill_process, [process])
                timer.start()

            output_lines = []

            try:
                # 实时输出，支持时间戳
                if process.stdout:
                    for line in iter(process.stdout.readline, ''):
                        if line:
                            line = line.rstrip()
                            output_lines.append(line)

                            if show_timestamp:
                                timestamp = datetime.now().strftime('%H:%M:%S')
                                formatted_line = f"[{timestamp}] {line}"
                            else:
                                formatted_line = line

                            print(formatted_line)
                            sys.stdout.flush()

                # 等待进程完成
                process.wait()

            finally:
                # 取消定时器
                if timer is not None:
                    timer.cancel()

            elapsed = time.time() - start_time
            end_datetime = datetime.now()

            # 检查是否超时
            if timeout is not None and (process.returncode is None or process.returncode < 0):
                print(f"⏰ [{end_datetime.strftime('%H:%M:%S')}] 命令执行超时({timeout}秒)，实际耗时: {elapsed:.2f}秒")
                raise BaseException(f"命令执行超时({timeout}秒)")

            # 检查返回码
            if process.returncode != 0:
                # 收集错误输出
                error_output = "\n".join(output_lines[-10:]) if output_lines else "无错误输出"
                raise BaseException(f'命令执行失败 (exit code: {process.returncode})\n错误详情: {error_output}')

            return process, process.returncode

        except KeyboardInterrupt:
            print(f"\n🛑 [{datetime.now().strftime('%H:%M:%S')}] 用户中断执行")
            if 'process' in locals() and process:
                process.terminate()
                process.wait()
            raise BaseException("用户中断执行")

        except Exception as e:
            elapsed = time.time() - start_time
            print(f"❌ [{datetime.now().strftime('%H:%M:%S')}] 命令执行异常: {e}")
            raise BaseException(f'命令执行失败: {e}')

import os
import platform
import math
from subprocess import Popen, PIPE, STDOUT
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient
from bson import ObjectId

if platform.system() == 'Windows':
    mongodump_exe = os.path.join('mongodb-database-tools', 'windows-x86_64-100.13.0', 'mongodump.exe')
    mongorestore_exe = os.path.join('mongodb-database-tools', 'windows-x86_64-100.13.0', 'mongorestore.exe')
elif platform.system() == 'Linux':
    mongodump_exe = os.path.join('mongodb-database-tools', 'rhel93-x86_64-100.13.0', 'mongodump')
    mongorestore_exe = os.path.join('mongodb-database-tools', 'rhel93-x86_64-100.13.0', 'mongorestore')
elif platform.system() == 'Darwin':
    mongodump_exe = os.path.join('mongodb-database-tools', 'macos-arm64-100.13.0', 'mongodump')
    mongorestore_exe = os.path.join('mongodb-database-tools', 'macos-arm64-100.13.0', 'mongorestore')


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


class MyDump(Shell):
    """
    导出数据库备份到目录
    """

    def __init__(self, mongo: Mongo, numParallelCollections: int = 4):
        super().__init__()
        self.mongo = mongo
        self.numParallelCollections = numParallelCollections

    def export_db(self, database, dump_root_path):
        """
        导出mongo数据库到dump目录
        :param database: 数据库名
        :param dump_root_path: dump根目录,会自动创建对应数据库名的dump目录
        :return:
        """
        auth_append = f'--username={self.mongo.username} --password="{self.mongo.password}" --authenticationDatabase=admin' if self.mongo.username else ''
        export_shell = (
            f'{mongodump_exe} '
            f'--host="{self.mongo.host}:{self.mongo.port}" '
            f'--db={database} '
            f'--out={dump_root_path} '
            f'--numParallelCollections={self.numParallelCollections} '
            f'--gzip {auth_append} '
        )
        self._exe_command(export_shell)

    def export_collection_partitioned_concurrent(self, database: str, collection: str, dump_root_path: str,
                                               partition_field: str = "_id", partitions: int = 4) -> List[str]:
        """
        并发分区导出大集合

        :param database: 数据库名
        :param collection: 集合名
        :param dump_root_path: dump根目录
        :param partition_field: 分区字段，默认为_id
        :param partitions: 分区数量
        :return: 导出的文件路径列表
        """
        # 构建MongoDB连接字符串
        if self.mongo.username and self.mongo.password:
            connection_string = f"mongodb://{self.mongo.username}:{self.mongo.password}@{self.mongo.host}:{self.mongo.port}/{database}?authSource=admin"
        else:
            connection_string = f"mongodb://{self.mongo.host}:{self.mongo.port}/{database}"

        client = MongoClient(connection_string)
        db = client[database]
        coll = db[collection]

        # 获取集合的文档总数
        total_docs = coll.count_documents({})
        if total_docs == 0:
            print(f"集合 {collection} 为空，跳过导出")
            client.close()
            return []

        docs_per_partition = total_docs // partitions

        # 获取最小和最大_id
        min_doc = coll.find_one(sort=[(partition_field, 1)])
        max_doc = coll.find_one(sort=[(partition_field, -1)])

        if not min_doc or not max_doc:
            print(f"集合 {collection} 无法获取文档范围")
            client.close()
            return []

        min_id = str(min_doc[partition_field])
        max_id = str(max_doc[partition_field])

        auth_append = f'--username={self.mongo.username} --password="{self.mongo.password}" --authenticationDatabase=admin' if self.mongo.username else ''

        # 构造所有导出任务
        export_tasks = []
        current_id = min_id

        for i in range(partitions):
            if i == partitions - 1:
                query_str = f'{{"{partition_field}": {{"$gte": {{"$oid": "{current_id}"}}}}}}'
            else:
                skip_docs = docs_per_partition * (i + 1)
                cursor = coll.find().sort(partition_field, 1).skip(skip_docs).limit(1)
                end_doc = next(cursor, None)
                if not end_doc:
                    break
                end_id = str(end_doc[partition_field])
                query_str = f'{{"{partition_field}": {{"$gte": {{"$oid": "{current_id}"}}, "$lt": {{"$oid": "{end_id}"}}}}}}'
                current_id = end_id

            output_dir = f"{dump_root_path}/{database}_{collection}_part{i}"
            export_cmd = (
                f'{mongodump_exe} '
                f'--host="{self.mongo.host}:{self.mongo.port}" '
                f'--db={database} '
                f'--collection={collection} '
                f'--query=\'{query_str}\' '
                f'--out={output_dir} '
                f'--gzip {auth_append} '
            )
            export_tasks.append((i + 1, export_cmd, output_dir))

        client.close()

        # 并发执行导出任务
        exported_dirs = []
        with ThreadPoolExecutor(max_workers=partitions) as executor:
            futures = [executor.submit(self._exe_command, cmd) for _, cmd, _ in export_tasks]
            for future, (idx, _, output_dir) in zip(as_completed(futures), export_tasks):
                try:
                    future.result()
                    print(f"✅ 分区 {idx}/{len(export_tasks)} 导出完成")
                    exported_dirs.append(output_dir)
                except Exception as e:
                    print(f"❌ 分区 {idx}/{len(export_tasks)} 导出失败: {e}")

        return exported_dirs

class MyImport(Shell):
    """
    从sql文件导入
    """

    def __init__(self, mongo: Mongo, numParallelCollections: int = 4, numInsertionWorkersPerCollection: int = 4):
        super().__init__()
        self.mongo = mongo
        self.numParallelCollections = numParallelCollections
        self.numInsertionWorkersPerCollection = numInsertionWorkersPerCollection

    def import_db(self, database, db_dir):
        """
        读取mongo导出的数据库目录文件并导入到mongo中
        :param db_dir: 数据库目录
        :return:
        """
        user_append = f'--username="{self.mongo.username}"' if self.mongo.username else ''
        password_append = f'--password="{self.mongo.password}"' if self.mongo.password else ''
        auth_database_append = f'--authenticationDatabase=admin' if self.mongo.username else ''
        import_shell = (
            f'{mongorestore_exe} '
            f'--host="{self.mongo.host}:{self.mongo.port}" {user_append} {password_append} {auth_database_append} '
            f'--numParallelCollections={self.numParallelCollections} '
            f'--numInsertionWorkersPerCollection={self.numInsertionWorkersPerCollection} '
            f'--noIndexRestore '
            f'--drop '
            f'--gzip '
            f'--db="{database}" {db_dir} '
        )
        self._exe_command(import_shell)

    def import_partitioned_collection(self, database: str, partition_dirs: List[str]) -> None:
        """
        并发导入分区导出的集合

        :param database: 数据库名
        :param partition_dirs: 分区导出目录列表
        """
        user_append = f'--username="{self.mongo.username}"' if self.mongo.username else ''
        password_append = f'--password="{self.mongo.password}"' if self.mongo.password else ''
        auth_database_append = f'--authenticationDatabase=admin' if self.mongo.username else ''

        # 并发导入所有分区
        import_commands = []
        for partition_dir in partition_dirs:
            import_cmd = (
                f'{mongorestore_exe} '
                f'--host="{self.mongo.host}:{self.mongo.port}" {user_append} {password_append} {auth_database_append} '
                f'--numParallelCollections=1 '  # 每个分区导入使用单线程，避免冲突
                f'--numInsertionWorkersPerCollection={self.numInsertionWorkersPerCollection} '
                f'--noIndexRestore '
                f'--gzip '
                f'--db="{database}" {partition_dir} '
            )
            import_commands.append(import_cmd)

        # 使用线程池并发执行导入
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=min(4, len(import_commands))) as executor:
            futures = [executor.submit(self._exe_command, cmd) for cmd in import_commands]
            for future in as_completed(futures):
                future.result()

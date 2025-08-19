import os
import platform
import math
from subprocess import Popen, PIPE, STDOUT
from typing import List, Tuple, Optional
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

    def export_collection_partitioned(self, database: str, collection: str, dump_root_path: str,
                                    partition_field: str = "_id", partitions: int = 4,
                                    partition_size: Optional[int] = None) -> List[str]:
        """
        分区并发导出大集合

        :param database: 数据库名
        :param collection: 集合名
        :param dump_root_path: dump根目录
        :param partition_field: 分区字段，默认为_id
        :param partitions: 分区数量
        :param partition_size: 每个分区的文档数量，如果提供则按数量分区
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

        # 计算分区大小
        if partition_size:
            partitions = math.ceil(total_docs / partition_size)

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

        # 生成查询条件并并发导出
        exported_files = []
        current_id = min_id

        auth_append = f'--username={self.mongo.username} --password="{self.mongo.password}" --authenticationDatabase=admin' if self.mongo.username else ''

        for i in range(partitions):
            if i == partitions - 1:
                # 最后一个分区包含剩余所有文档
                query_filter = {partition_field: {"$gte": ObjectId(current_id)}}
                query_str = f'{{"{partition_field}": {{"$gte": ObjectId("{current_id}")}}}}'
            else:
                # 计算当前分区的结束_id
                skip_docs = docs_per_partition * (i + 1)
                cursor = coll.find().sort(partition_field, 1).skip(skip_docs).limit(1)
                end_doc = next(cursor, None)

                if not end_doc:
                    break

                end_id = str(end_doc[partition_field])
                query_filter = {partition_field: {"$gte": ObjectId(current_id), "$lt": ObjectId(end_id)}}
                query_str = f'{{"{partition_field}": {{"$gte": ObjectId("{current_id}"), "$lt": ObjectId("{end_id}")}}}}'
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

            print(f"导出分区 {i+1}/{partitions}: {query_str}")
            self._exe_command(export_cmd)
            exported_files.append(output_dir)

        client.close()
        return exported_files

    def export_large_collection(self, database: str, collection: str, dump_root_path: str,
                              threshold_docs: int = 1000000, partition_field: str = "_id") -> bool:
        """
        智能判断并导出大集合

        :param database: 数据库名
        :param collection: 集合名
        :param dump_root_path: dump根目录
        :param threshold_docs: 触发分区导出的文档数量阈值
        :param partition_field: 分区字段
        :return: 是否使用了分区导出
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

        client.close()

        auth_append = f'--username={self.mongo.username} --password="{self.mongo.password}" --authenticationDatabase=admin' if self.mongo.username else ''

        if total_docs > threshold_docs:
            print(f"集合 {collection} 有 {total_docs} 条文档，使用分区并发导出")
            partitions = min(8, max(2, total_docs // 500000))  # 每50万文档一个分区，最多8个
            self.export_collection_partitioned(database, collection, dump_root_path,
                                           partition_field, partitions=partitions)
            return True
        else:
            print(f"集合 {collection} 有 {total_docs} 条文档，使用普通导出")
            output_dir = f"{dump_root_path}/{database}"
            export_cmd = (
                f'{mongodump_exe} '
                f'--host="{self.mongo.host}:{self.mongo.port}" '
                f'--db={database} '
                f'--collection={collection} '
                f'--out={output_dir} '
                f'--gzip {auth_append} '
            )
            self._exe_command(export_cmd)
            return False


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

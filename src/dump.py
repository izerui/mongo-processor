import os
import platform
import math
from subprocess import Popen, PIPE, STDOUT
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient
from bson import ObjectId

from base import Shell, Mongo, mongodump_exe


class MyDump(Shell):
    """
    导出数据库备份到目录
    """

    def __init__(self, mongo: Mongo, numParallelCollections: int = 4):
        super().__init__()
        self.mongo = mongo
        self.numParallelCollections = numParallelCollections

    def export_db(self, database: str, dump_root_path: str, threshold_docs: int = 1000000) -> List[str]:
        """
        智能导出数据库：自动判断是否使用分区导出
        :param database: 数据库名
        :param dump_root_path: 导出根目录
        :param threshold_docs: 触发分区导出的文档数量阈值
        :return: 所有导出的目录路径列表（含分区）
        """
        # 获取大集合信息
        large_collections = self._get_large_collections(database, threshold_docs)
        all_dirs = []

        # 获取所有集合
        try:
            if self.mongo.username and self.mongo.password:
                uri = f"mongodb://{self.mongo.username}:{self.mongo.password}@{self.mongo.host}:{self.mongo.port}/{database}?authSource=admin"
            else:
                uri = f"mongodb://{self.mongo.host}:{self.mongo.port}/{database}"
            client = MongoClient(uri)
            db = client[database]
            all_collections = db.list_collection_names()
            client.close()

            # 处理每个集合
            for collection in all_collections:
                # 检查是否是大集合
                is_large = any(c[0] == collection for c in large_collections)

                if is_large:
                    # 大集合使用分区导出
                    doc_count = next(c[1] for c in large_collections if c[0] == collection)
                    partitions = min(8, max(2, doc_count // 500000))
                    self.export_collection_partitioned_concurrent(
                        database=database,
                        collection=collection,
                        dump_root_path=dump_root_path,
                        partition_field="_id",
                        partitions=partitions,
                        max_workers=max_workers
                    )
                    print(f'✅ 大集合 {collection} 分区导出完成')
                else:
                    # 小集合标准导出
                    output_dir = os.path.join(dump_root_path, database, collection)
                    auth_append = f'--username={self.mongo.username} --password="{self.mongo.password}" --authenticationDatabase=admin' if self.mongo.username else ''
                    export_shell = (
                        f'{mongodump_exe} '
                        f'--host="{self.mongo.host}:{self.mongo.port}" '
                        f'--db={database} '
                        f'--collection={collection} '
                        f'--out={output_dir} '
                        f'--numParallelCollections={self.numParallelCollections} '
                        f'--gzip {auth_append}'
                    )
                    self._exe_command(export_shell)
                    print(f'✅ 小集合 {collection} 导出完成')

        except Exception as e:
            print(f"⚠️ 导出数据库 {database} 失败: {e}")

        # 只返回数据库目录路径
        db_dir = os.path.join(dump_root_path, database)
        return [db_dir]


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

            output_dir = f"{dump_root_path}/{database}/{collection}/part{i:03d}"
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
                    print(f"✅ 集合 {collection} 分区 {idx}/{len(export_tasks)} 导出完成")
                    exported_dirs.append(output_dir)
                except Exception as e:
                    print(f"❌ 集合 {collection} 分区 {idx}/{len(export_tasks)} 导出失败: {e}")

        return [os.path.join(dump_root_path, database, collection)]

    def _get_large_collections(self, database: str, threshold_docs: int) -> List[Tuple[str, int]]:
        """获取大集合列表"""
        try:
            if self.mongo.username and self.mongo.password:
                uri = f"mongodb://{self.mongo.username}:{self.mongo.password}@{self.mongo.host}:{self.mongo.port}/{database}?authSource=admin"
            else:
                uri = f"mongodb://{self.mongo.host}:{self.mongo.port}/{database}"
            client = MongoClient(uri)
            db = client[database]
            collections = db.list_collection_names()
            large = []
            for name in collections:
                count = db[name].count_documents({})
                if count >= threshold_docs:
                    large.append((name, count))
            client.close()
            return large
        except Exception as e:
            print(f"⚠️ 获取大集合失败: {e}")
            return []

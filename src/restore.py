import os
import platform
import re
from subprocess import Popen, PIPE, STDOUT
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient

from base import Shell, Mongo, mongorestore_exe


class MyRestore(Shell):
    """
    按数据库整体导入
    """

    def __init__(self, mongo: Mongo, num_parallel_collections: int = 4, num_insertion_workers: int = 4):
        super().__init__()
        self.mongo = mongo
        self.num_parallel_collections = num_parallel_collections
        self.num_insertion_workers = num_insertion_workers

    def restore_db(self, database: str, dump_root_path: str) -> None:
        """
        按数据库整体导入
        :param database: 目标数据库名
        :param dump_dirs: 导出的数据库目录路径列表
        """
        if not dump_root_path:
            print(f"⚠️ 没有提供数据库目录路径")
            return

        db_dir = os.path.join(dump_root_path, database)

        if not os.path.exists(db_dir):
            print(f"⚠️ 数据库目录不存在: {db_dir}")
            return

        try:
            # 构建认证参数
            user_append = f'--username="{self.mongo.username}"' if self.mongo.username else ''
            password_append = f'--password="{self.mongo.password}"' if self.mongo.password else ''
            auth_append = f'--authenticationDatabase=admin' if self.mongo.username else ''

            # 构建导入命令 - 直接导入整个数据库
            import_cmd = (
                f'{mongorestore_exe} '
                f'--host="{self.mongo.host}:{self.mongo.port}" '
                f'{user_append} {password_append} {auth_append} '
                f'--numParallelCollections={self.num_parallel_collections} '
                f'--numInsertionWorkersPerCollection={self.num_insertion_workers} '
                f'--noIndexRestore '
                f'--drop '
                f'--db={database} '
                f'--dir="{os.path.join(dump_root_path, database)}"'
            )

            # 执行导入
            self._exe_command(import_cmd)
            print(f'✅ 数据库 {database} 导入完成')

            # 自动合并所有分片集合
            print("🔄 开始自动合并所有分片集合...")
            self.merge_all_sharded_collections(database)

        except Exception as e:
            print(f'❌ 导入数据库 {database} 失败: {e}')
            raise

    def merge_sharded_collections(self, database: str, original_collection_name: str) -> None:
        """
        通过聚合管道将分片的多个集合合并回原始集合名
        :param database: 数据库名
        :param original_collection_name: 原始集合名（不含分片后缀）
        """
        client = None
        try:
            client = MongoClient(self.mongo.host, int(self.mongo.port))
            if self.mongo.username and self.mongo.password:
                client.admin.authenticate(self.mongo.username, self.mongo.password)

            db = client[database]

            # 获取所有分片集合
            pattern = re.compile(f"^{original_collection_name}_part\\d+$")
            shard_collections = []

            for collection_name in db.list_collection_names():
                if pattern.match(collection_name):
                    shard_collections.append(collection_name)

            if not shard_collections:
                print(f"⚠️ 未找到分片集合: {original_collection_name}_part*")
                return

            print(f"📊 找到 {len(shard_collections)} 个分片集合: {shard_collections}")

            # 创建目标集合（如果已存在则先删除）
            target_collection = original_collection_name
            if target_collection in db.list_collection_names():
                db[target_collection].drop()
                print(f"🗑️ 已删除现有集合: {target_collection}")

            # 使用聚合管道合并所有分片集合
            pipeline = []

            # 添加所有分片集合的$unionWith阶段
            for i, shard_collection in enumerate(shard_collections):
                if i == 0:
                    # 第一个集合作为主集合
                    pipeline.append({"$match": {}})
                else:
                    # 后续集合使用$unionWith合并
                    pipeline.append({"$unionWith": {"coll": shard_collection}})

            # 执行聚合管道
            result = db[shard_collections[0]].aggregate(pipeline, allowDiskUse=True)

            # 将结果插入到目标集合
            bulk_ops = []
            count = 0

            for doc in result:
                bulk_ops.append(doc)
                count += 1

                # 批量插入，每1000条执行一次
                if len(bulk_ops) >= 10000:
                    db[target_collection].insert_many(bulk_ops, ordered=False)
                    bulk_ops = []

            # 插入剩余文档
            if bulk_ops:
                db[target_collection].insert_many(bulk_ops, ordered=False)

            print(f"✅ 合并完成: {len(shard_collections)} 个分片集合 -> {target_collection}，共 {count} 条文档")

            # 自动删除分片集合
            for shard_collection in shard_collections:
                db[shard_collection].drop()
            print(f"🗑️ 已自动删除 {len(shard_collections)} 个分片集合")

        except Exception as e:
            print(f"❌ 合并分片集合失败: {e}")
            raise
        finally:
            if client:
                client.close()

    def merge_all_sharded_collections(self, database: str) -> None:
        """
        自动发现并合并数据库中所有的分片集合
        :param database: 数据库名
        """
        client = None
        try:
            client = MongoClient(self.mongo.host, int(self.mongo.port))
            if self.mongo.username and self.mongo.password:
                client.admin.authenticate(self.mongo.username, self.mongo.password)

            db = client[database]

            # 获取所有集合名
            all_collections = db.list_collection_names()

            # 找出所有分片集合的原始集合名
            sharded_originals = set()
            pattern = re.compile(r"^(.*)_part\d+$")

            for collection_name in all_collections:
                match = pattern.match(collection_name)
                if match:
                    original_name = match.group(1)
                    sharded_originals.add(original_name)

            if not sharded_originals:
                print("⚠️ 未找到任何分片集合")
                return

            print(f"📊 发现 {len(sharded_originals)} 个需要合并的分片集合组")

            # 合并每个原始集合
            for original_name in sorted(sharded_originals):
                print(f"\n🔄 开始合并: {original_name}")
                self.merge_sharded_collections(database, original_name)

        except Exception as e:
            print(f"❌ 自动合并失败: {e}")
            raise
        finally:
            if client:
                client.close()

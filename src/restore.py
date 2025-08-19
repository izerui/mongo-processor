import os
import platform
from subprocess import Popen, PIPE, STDOUT
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient

from base import Shell, Mongo, mongorestore_exe


class MyRestore(Shell):
    """
    从 mongodump 导出的目录或分区目录导入到 MongoDB
    """

    def __init__(self, mongo: Mongo, num_parallel_collections: int = 4, num_insertion_workers: int = 4):
        super().__init__()
        self.mongo = mongo
        self.num_parallel_collections = num_parallel_collections
        self.num_insertion_workers = num_insertion_workers

    def import_db(self, database: str, db_dir: str):
        """
        导入整个数据库目录
        :param database: 目标数据库名
        :param db_dir: 导出的数据库目录路径
        """
        user_append = f'--username="{self.mongo.username}"' if self.mongo.username else ''
        password_append = f'--password="{self.mongo.password}"' if self.mongo.password else ''
        auth_append = f'--authenticationDatabase=admin' if self.mongo.username else ''

        import_cmd = (
            f'{mongorestore_exe} '
            f'--host="{self.mongo.host}:{self.mongo.port}" '
            f'{user_append} {password_append} {auth_append} '
            f'--numParallelCollections={self.num_parallel_collections} '
            f'--numInsertionWorkersPerCollection={self.num_insertion_workers} '
            f'--noIndexRestore '
            f'--drop '
            f'--gzip '
            f'--dir="{db_dir}"'
        )
        self._exe_command(import_cmd)

    def import_partitioned_collection(self, database: str, partition_dirs: List[str], collection: str, max_workers: int = 4):
        """
        并发导入分区导出的集合
        :param database: 目标数据库名
        :param partition_dirs: 分区目录列表
        :param collection: 集合名称
        :param max_workers: 最大并发线程数
        """
        user_append = f'--username="{self.mongo.username}"' if self.mongo.username else ''
        password_append = f'--password="{self.mongo.password}"' if self.mongo.password else ''
        auth_append = f'--authenticationDatabase=admin' if self.mongo.username else ''

        import_tasks = []
        for i, partition_dir in enumerate(partition_dirs):
            if not os.path.exists(partition_dir):
                print(f"⚠️ 分区目录不存在，跳过: {partition_dir}")
                continue
            # 使用 --nsInclude 和 --dir 参数
            import_cmd = (
                f'{mongorestore_exe} '
                f'--host="{self.mongo.host}:{self.mongo.port}" '
                f'{user_append} {password_append} {auth_append} '
                f'--numParallelCollections=1 '
                f'--numInsertionWorkersPerCollection={self.num_insertion_workers} '
                f'--noIndexRestore '
                f'--drop '
                f'--gzip '
                f'--nsInclude="{database}.{collection}" '
                f'--dir="{partition_dir}"'
            )
            import_tasks.append((i + 1, import_cmd, partition_dir, collection))

        if not import_tasks:
            print(f"⚠️ 没有任何分区需要导入: {database}")
            return

        print(f"🔄 开始并发导入 {len(import_tasks)} 个分区到数据库: {database}")

        with ThreadPoolExecutor(max_workers=min(max_workers, len(import_tasks))) as executor:
            futures = {executor.submit(self._exe_command, cmd): (idx, dir_path, coll) for idx, cmd, dir_path, coll in import_tasks}
            for future in as_completed(futures):
                idx, dir_path, coll_name = futures[future]
                try:
                    future.result()
                    print(f"✅ 集合 {coll_name} 分区 {idx}/{len(import_tasks)} 导入成功: {dir_path}")
                except Exception as e:
                    print(f"❌ 集合 {coll_name} 分区 {idx}/{len(import_tasks)} 导入失败: {e}")

        print(f"✅ 所有分区导入完成: {database}")

    def restore_db(self, database: str, dump_dirs: List[str]) -> None:
        """
        智能导入数据库：根据新的统一目录结构导入
        :param database: 目标数据库名
        :param dump_dirs: 导出的目录列表（实际为数据库目录）
        """
        # 新的目录结构：dumps/数据库名/集合名/[part001|part002|文件]
        db_base_dir = os.path.join(os.path.dirname(dump_dirs[0]) if dump_dirs else "", database)

        if not os.path.exists(db_base_dir):
            print(f"⚠️ 数据库目录不存在: {db_base_dir}")
            return

        # 获取所有集合目录
        collections = []
        for item in os.listdir(db_base_dir):
            collection_dir = os.path.join(db_base_dir, item)
            if os.path.isdir(collection_dir):
                collections.append(item)

        if not collections:
            print(f"⚠️ 没有找到任何集合: {db_base_dir}")
            return

        print(f"🔄 开始导入数据库 {database}，共 {len(collections)} 个集合")

        # 处理每个集合
        for collection in collections:
            collection_dir = os.path.join(db_base_dir, collection)

            # 检查是否有分区目录
            partition_dirs = []
            for item in os.listdir(collection_dir):
                item_path = os.path.join(collection_dir, item)
                if os.path.isdir(item_path) and item.startswith('part'):
                    partition_dirs.append(item_path)

            if partition_dirs:
                # 有分区，使用分区导入
                print(f"🔄 集合 {collection} 有 {len(partition_dirs)} 个分区")
                self.import_partitioned_collection(database, partition_dirs, collection)
            else:
                # 无分区，直接导入
                print(f"🔄 导入集合 {collection}: {collection_dir}")
                self.import_db(database, collection_dir)

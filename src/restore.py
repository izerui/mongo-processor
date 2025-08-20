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
        高性能直接导入：自动识别分片集合并直接导入到原始集合
        :param database: 目标数据库名
        :param dump_root_path: 导出根路径
        """
        self.restore_db_direct(database, dump_root_path)

    def _import_single_file(self, database: str, target_collection: str, file_path: str, is_first_shard: bool = False) -> str:
        """
        导入单个文件到指定集合
        :param database: 数据库名
        :param target_collection: 目标集合名
        :param file_path: 文件路径
        :param is_first_shard: 是否是第一个分片（决定是否drop）
        :return: 导入结果描述
        """
        try:
            # 构建认证参数
            user_append = f'--username="{self.mongo.username}"' if self.mongo.username else ''
            password_append = f'--password="{self.mongo.password}"' if self.mongo.password else ''
            auth_append = f'--authenticationDatabase=admin' if self.mongo.username else ''

            import_cmd = (
                f'{mongorestore_exe} '
                f'--host {self.mongo.host}:{self.mongo.port} '
                f'{user_append} {password_append} {auth_append} '
                f'--numParallelCollections=1 '
                f'--numInsertionWorkersPerCollection={self.num_insertion_workers} '
                f'--noIndexRestore '
                f'{"--drop " if is_first_shard else ""}'
                f'--db {database} '
                f'--collection {target_collection} '
                f'"{file_path}"'
            )

            self._exe_command(import_cmd)
            file_name = os.path.basename(file_path)
            return f"{file_name} -> {target_collection}"

        except Exception as e:
            raise Exception(f"导入 {file_path} 失败: {e}")

    def restore_db_direct(self, database: str, dump_root_path: str) -> None:
        """
        高性能直接导入：自动识别分片集合并直接导入到原始集合
        :param database: 目标数据库名
        :param dump_root_path: 导出根路径
        """
        if not dump_root_path:
            print(f"⚠️ 没有提供数据库目录路径")
            return

        db_dir = os.path.join(dump_root_path, database)
        if not os.path.exists(db_dir):
            print(f"⚠️ 数据库目录不存在: {db_dir}")
            return

        try:
            # 获取所有文件
            all_files = os.listdir(db_dir)

            # 识别分片集合和普通集合
            sharded_collections = {}
            normal_collections = []
            import_tasks = []

            for file in all_files:
                if file.endswith('.bson'):
                    collection_name = file.replace('.bson', '')
                    if '_part' in collection_name:
                        # 分片集合
                        original_name = collection_name.split('_part')[0]
                        if original_name not in sharded_collections:
                            sharded_collections[original_name] = []
                        sharded_collections[original_name].append(os.path.join(db_dir, file))
                    else:
                        # 普通集合
                        normal_collections.append(collection_name)

            # 构建所有导入任务
            # 分片任务：每个分片文件导入到原始集合
            for original_name, shard_files in sharded_collections.items():
                for i, shard_file in enumerate(shard_files):
                    is_first = (i == 0)  # 只有第一个分片文件使用--drop
                    import_tasks.append(('sharded', original_name, shard_file, is_first))

            # 普通任务：每个普通集合单独导入
            for collection_name in normal_collections:
                bson_file = os.path.join(db_dir, f"{collection_name}.bson")
                if os.path.exists(bson_file):
                    import_tasks.append(('normal', collection_name, bson_file, True))

            if not import_tasks:
                print("⚠️ 没有找到需要导入的文件")
                return

            print(f"🚀 准备并发导入 {len(import_tasks)} 个任务，使用 {self.num_parallel_collections} 个线程")

            # 使用线程池并发导入
            print(f"📁 数据库目录: {db_dir}")
            print(f"📊 分片集合: {list(sharded_collections.keys())}")
            print(f"📊 普通集合: {normal_collections}")

            with ThreadPoolExecutor(max_workers=self.num_parallel_collections) as executor:
                future_to_task = {}

                for task_type, target_collection, file_path, is_first in import_tasks:
                    # 对于普通集合，总是使用drop；对于分片，只有第一个文件使用drop
                    should_drop = is_first and (task_type == 'sharded' or task_type == 'normal')
                    future = executor.submit(self._import_single_file, database, target_collection, file_path, should_drop)
                    future_to_task[future] = (task_type, target_collection, file_path)

                # 收集结果
                completed = 0
                for future in as_completed(future_to_task):
                    task_type, target_collection, file_path = future_to_task[future]
                    try:
                        result = future.result()
                        completed += 1
                        print(f"✅ [{completed}/{len(import_tasks)}] {result}")
                    except Exception as e:
                        print(f"❌ 导入失败 {file_path}: {e}")
                        # 如果是认证失败，给出提示
                        if "authentication" in str(e).lower():
                            print("🔑 请检查用户名、密码和认证数据库设置")
                        elif "connection" in str(e).lower():
                            print("🔗 请检查MongoDB连接参数")
                        elif "file" in str(e).lower():
                            print("📁 请检查文件路径和权限")
                        raise

            print(f'✅ 数据库 {database} 并发导入完成')

        except Exception as e:
            print(f'❌ 直接导入数据库 {database} 失败: {e}')
            raise

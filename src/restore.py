import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from base import MyMongo, MongoConfig
from src.base import GlobalConfig


class MyRestore(MyMongo):
    """
    按数据库整体导入
    """

    def __init__(self, mongo: MongoConfig, global_config: GlobalConfig):
        super().__init__(mongo, global_config)

    def restore_db(self, database: str) -> None:
        """
        高性能直接导入：自动识别分片集合并直接导入到原始集合
        :param database: 目标数据库名
        """
        db_dir = os.path.join(self.global_config.dump_root_path, database)
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

            # 过滤忽略的集合
            for file in all_files:
                if file.endswith('.bson'):
                    collection_name = file.replace('.bson', '')

                    # 检查是否是被忽略的集合
                    full_name = f"{database}.{collection_name}"
                    if full_name in self.global_config.ignore_collections:
                        print(f"🚫 跳过导入（被忽略）: {full_name}")
                        continue

                    if '_part' in collection_name:
                        # 分片集合
                        original_name = collection_name.split('_part')[0]
                        # 检查分片集合的原始名称是否被忽略
                        original_full_name = f"{database}.{original_name}"
                        if original_full_name in self.global_config.ignore_collections:
                            print(f"🚫 跳过分片导入（被忽略）: {original_full_name}")
                            continue

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
                    import_tasks.append(('sharded', original_name, shard_file))

            # 先重建原始集合（仅针对分片集合）
            for original_name in sharded_collections:
                self.client[database][original_name].drop()
                self.client[database].create_collection(original_name)

            # 普通任务：每个普通集合单独导入
            for collection_name in normal_collections:
                bson_file = os.path.join(db_dir, f"{collection_name}.bson")
                if os.path.exists(bson_file):
                    import_tasks.append(('normal', collection_name, bson_file))

            if not import_tasks:
                print("⚠️ 没有找到需要导入的文件（可能全部被忽略或目录为空）")
                return

            print(f"🚀 准备并发导入 {len(import_tasks)} 个任务，使用 {self.global_config.numParallelCollections} 个线程")

            # 使用线程池并发导入
            print(f"📁 数据库目录: {db_dir}")
            print(f"📊 分片集合: {list(sharded_collections.keys())}")
            print(f"📊 普通集合: {normal_collections}")

            with ThreadPoolExecutor(max_workers=self.global_config.numParallelCollections) as executor:
                future_to_task = {}

                for task_type, target_collection, file_path in import_tasks:
                    # 对于普通集合，总是使用drop；对于分片，只有第一个文件使用drop
                    future = executor.submit(self._import_single_file, database, target_collection, file_path,
                                             task_type == 'sharded')
                    future_to_task[future] = (task_type, target_collection, file_path)

                # 收集结果
                completed = 0
                errors = []

                for future in as_completed(future_to_task, timeout=None):  # 无超时限制
                    task_type, target_collection, file_path = future_to_task[future]
                    try:
                        result = future.result()
                        completed += 1
                        print(f"✅ 集合导入成功: {database}: [{completed}/{len(import_tasks)}] {result}")
                    except Exception as e:
                        print(f"❌ 导入失败 {file_path}: {e}")
                        # 如果是认证失败，给出提示
                        if "authentication" in str(e).lower():
                            print("🔑 请检查用户名、密码和认证数据库设置")
                        elif "connection" in str(e).lower():
                            print("🔗 请检查MongoDB连接参数")
                        elif "file" in str(e).lower():
                            print("📁 请检查文件路径和权限")
                        elif "timeout" in str(e).lower():
                            print("⏰ 命令执行超时，可尝试增加超时时间")
                        errors.append(f"{file_path}: {e}")

                # 如果有错误，汇总后抛出
                if errors:
                    raise Exception(f"导入过程中出现 {len(errors)} 个错误:\n" + "\n".join(errors))


        except Exception as e:
            print(f'❌ 导入数据库 {database} 失败: {e}')
            raise

    def _import_single_file(self, database: str, target_collection: str, file_path: str,
                            is_sharded: bool = False) -> str:
        """
        导入单个文件到指定集合
        :param database: 数据库名
        :param target_collection: 目标集合名
        :param file_path: 文件路径
        :param is_sharded: 是否是分片
        :return: 导入结果描述
        """
        try:
            # 构建认证参数
            user_append = f'--username="{self.mongo_config.username}"' if self.mongo_config.username else ''
            password_append = f'--password="{self.mongo_config.password}"' if self.mongo_config.password else ''
            auth_append = f'--authenticationDatabase=admin' if self.mongo_config.username else ''

            import_cmd = (
                f'{self.mongorestore_exe} '
                f'--host {self.mongo_config.host}:{self.mongo_config.port} '
                f'{user_append} {password_append} {auth_append} '
                f'--numParallelCollections=1 '
                f'--numInsertionWorkersPerCollection={self.global_config.numInsertionWorkersPerCollection} '
                f'--noIndexRestore '
                f'{"--drop " if not is_sharded else ""}'
                f'--db {database} '
                f'--collection {target_collection} '
                f'"{file_path}"'
            )

            self.exe_command(import_cmd, timeout=None)
            file_name = os.path.basename(file_path)
            return f"{file_name} -> {target_collection}"
        except Exception as e:
            raise Exception(f"导入 {file_path} 失败: {e}")

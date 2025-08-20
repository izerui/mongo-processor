#!/usr/bin/env python3
"""
数据库恢复类 - 支持分片数据恢复
集成了分片识别、元数据处理和恢复逻辑
"""

import os
import json
import math
from datetime import datetime
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient
from pymongo.collection import Collection

from base import Shell, Mongo, mongorestore_exe, ShardConfig


class MyRestore(Shell):
    """数据库恢复类"""

    def __init__(self, mongo: Mongo, num_parallel_collections: int = 4, num_insertion_workers: int = 4,
                 enable_sharding: bool = True, shard_config: Optional[ShardConfig] = None):
        super().__init__()
        self.mongo = mongo
        self.num_parallel_collections = num_parallel_collections
        self.num_insertion_workers = num_insertion_workers
        self.enable_sharding = enable_sharding
        self.shard_config = shard_config or ShardConfig()
        self.client = None

    def restore_db(self, database: str, dump_root_path: str) -> None:
        """
        按数据库整体导入，支持分片数据恢复
        :param database: 目标数据库名
        :param dump_root_path: 导出的根目录路径
        """
        try:
            print(f"🚀 开始恢复数据库: {database}")

            if self.enable_sharding:
                self._restore_db_with_sharding(database, dump_root_path)
            else:
                self._restore_db_normal(database, dump_root_path)

            print(f"🎉 数据库 {database} 恢复完成")

        except Exception as e:
            print(f'❌ 恢复数据库 {database} 失败: {e}')
            raise

    def _restore_db_with_sharding(self, database: str, dump_root_path: str):
        """
        支持分片的数据库恢复
        """
        try:
            db_dump_dir = os.path.join(dump_root_path, database)

            if not os.path.exists(db_dump_dir):
                print(f"⚠️ 数据库目录不存在: {db_dump_dir}")
                return

            # 获取所有需要恢复的collection
            collections = self._get_collections_from_dump(db_dump_dir)
            if not collections:
                print(f"⚠️ 数据库目录 {db_dump_dir} 中没有找到collection文件")
                return

            print(f"📊 发现 {len(collections)} 个collection需要恢复")

            # 分析哪些collection需要分片恢复
            collections_to_shard = []
            collections_normal = []

            for collection_name in collections:
                metadata = self._read_shard_metadata(database, collection_name, db_dump_dir)
                if metadata:
                    collections_to_shard.append((collection_name, metadata))
                    print(f"🔄 Collection {collection_name} 将使用分片恢复")
                else:
                    collections_normal.append(collection_name)
                    print(f"📦 Collection {collection_name} 使用常规恢复")

            # 并发处理所有collection
            with ThreadPoolExecutor(max_workers=self.num_parallel_collections) as executor:
                futures = []

                # 提交分片collection的恢复任务
                for collection_name, metadata in collections_to_shard:
                    future = executor.submit(
                        self._restore_collection_with_shards,
                        database, collection_name, db_dump_dir, metadata
                    )
                    futures.append((future, collection_name, True))  # True表示分片

                # 提交常规collection的恢复任务
                for collection_name in collections_normal:
                    future = executor.submit(
                        self._restore_collection_normal,
                        database, collection_name, db_dump_dir
                    )
                    futures.append((future, collection_name, False))  # False表示常规

                # 等待所有任务完成
                for future, collection_name, is_sharded in futures:
                    try:
                        future.result()
                        restore_type = "分片" if is_sharded else "常规"
                        print(f"✅ Collection {collection_name} {restore_type}恢复完成")
                    except Exception as e:
                        print(f"❌ Collection {collection_name} 恢复失败: {e}")
                        raise

            print(f"🎉 数据库 {database} 分片恢复完成")

        except Exception as e:
            print(f'❌ 分片恢复数据库 {database} 失败: {e}')
            raise
        finally:
            self._disconnect()

    def _restore_db_normal(self, database: str, dump_root_path: str):
        """
        常规数据库恢复（不使用分片）
        """
        try:
            db_dump_dir = os.path.join(dump_root_path, database)

            if not os.path.exists(db_dump_dir):
                print(f"⚠️ 数据库目录不存在: {db_dump_dir}")
                return

            # 构建认证参数
            user_append = f'--username="{self.mongo.username}"' if self.mongo.username else ''
            password_append = f'--password="{self.mongo.password}"' if self.mongo.password else ''
            auth_append = f'--authenticationDatabase=admin' if self.mongo.username else ''

            # 构建导入命令 - 使用--db参数指定数据库
            import_cmd = (
                f'{mongorestore_exe} '
                f'--host="{self.mongo.host}:{self.mongo.port}" '
                f'{user_append} {password_append} {auth_append} '
                f'--db={database} '
                f'--numParallelCollections={self.num_parallel_collections} '
                f'--numInsertionWorkersPerCollection={self.num_insertion_workers} '
                f'--noIndexRestore '
                f'--drop '
                f'--dir="{db_dump_dir}"'
            )

            # 执行导入
            self._exe_command(import_cmd)

            print(f"✅ 数据库 {database} 常规恢复完成")

        except Exception as e:
            print(f'❌ 常规恢复数据库 {database} 失败: {e}')
            raise

    def _restore_collection_with_shards(self, db_name: str, collection_name: str, db_dump_dir: str, metadata: Dict[str, Any]):
        """使用分片数据恢复单个collection - 直接导入分片文件，无需合并"""
        try:
            shard_count = metadata['shard_count']
            print(f"🔄 开始恢复 {collection_name} 的 {shard_count} 个分片")

            # 构建认证参数
            user_append = f'--username="{self.mongo.username}"' if self.mongo.username else ''
            password_append = f'--password="{self.mongo.password}"' if self.mongo.password else ''
            auth_append = f'--authenticationDatabase=admin' if self.mongo.username else ''

            # 并发恢复所有分片
            with ThreadPoolExecutor(max_workers=self.shard_config.shard_concurrency) as executor:
                futures = []
                for i in range(shard_count):
                    shard_suffix = f"_shard_{i:03d}"
                    shard_collection_name = f"{collection_name}{shard_suffix}"

                    shard_bson = os.path.join(db_dump_dir, f"{shard_collection_name}.bson")
                    if not os.path.exists(shard_bson):
                        raise Exception(f"分片恢复失败: 文件不存在 {shard_bson}")

                    file_size = os.path.getsize(shard_bson)
                    if file_size == 0:
                        raise Exception(f"分片恢复失败: 文件为空 {shard_bson} (大小: {file_size} 字节)")

                    # 提交分片恢复任务
                    restore_cmd = (
                        f'{mongorestore_exe} '
                        f'--host="{self.mongo.host}:{self.mongo.port}" '
                        f'{user_append} {password_append} {auth_append} '
                        f'--db={db_name} '
                        f'--collection={collection_name} '
                        f'--numInsertionWorkersPerCollection={self.num_insertion_workers} '
                        f'--noIndexRestore '
                        f'--drop '
                        f'"{shard_bson}"'
                    )

                    future = executor.submit(self._exe_command, restore_cmd)
                    futures.append((future, i))

                # 等待所有分片恢复完成
                for future, i in futures:
                    try:
                        future.result()
                        print(f"✅ 分片 {i + 1}/{shard_count} 恢复完成")
                    except Exception as e:
                        print(f"❌ 分片 {i + 1} 恢复失败: {e}")
                        raise

            print(f"✅ Collection {collection_name} 分片恢复完成")

        except Exception as e:
            print(f"❌ 分片恢复collection {collection_name} 失败: {e}")
            raise

        except Exception as e:
            print(f"❌ 分片恢复collection {collection_name} 失败: {e}")
            raise

    def _restore_collection_normal(self, db_name: str, collection_name: str, db_dump_dir: str):
        """常规恢复单个collection（不分片）"""
        try:
            collection_bson = os.path.join(db_dump_dir, f"{collection_name}.bson")
            collection_metadata = os.path.join(db_dump_dir, f"{collection_name}.metadata.json")

            if not os.path.exists(collection_bson):
                raise Exception(f"常规恢复失败: 文件不存在 {collection_bson}")

            file_size = os.path.getsize(collection_bson)
            if file_size == 0:
                raise Exception(f"常规恢复失败: 文件为空 {collection_bson} (大小: {file_size} 字节)")

            print(f"✅ 常规恢复文件检查通过: {collection_bson} (大小: {file_size} 字节)")

            # 构建认证参数
            user_append = f'--username="{self.mongo.username}"' if self.mongo.username else ''
            password_append = f'--password="{self.mongo.password}"' if self.mongo.password else ''
            auth_append = f'--authenticationDatabase=admin' if self.mongo.username else ''

            # 构建恢复命令
            restore_cmd = (
                f'{mongorestore_exe} '
                f'--host="{self.mongo.host}:{self.mongo.port}" '
                f'{user_append} {password_append} {auth_append} '
                f'--db={db_name} '
                f'--collection={collection_name} '
                f'--numInsertionWorkersPerCollection={self.num_insertion_workers} '
                f'--noIndexRestore '
                f'--drop '
                f'"{collection_bson}"'
            )

            # 执行恢复
            self._exe_command(restore_cmd)

            print(f"✅ Collection {collection_name} 常规恢复完成")

        except Exception as e:
            print(f"❌ 常规恢复collection {collection_name} 失败: {e}")
            raise

    # 移除 _merge_temp_collections 方法，不再需要

    def _get_collections_from_dump(self, db_dir: str) -> List[str]:
        """从导出目录中获取所有collection名称"""
        collections = set()
        try:
            for filename in os.listdir(db_dir):
                if filename.endswith('.bson'):
                    # 提取collection名称，处理分片情况
                    collection_name = filename[:-5]  # 去掉.bson后缀

                    # 如果是分片文件，提取原始collection名称
                    if '_shard_' in collection_name:
                        original_name = collection_name.split('_shard_')[0]
                        collections.add(original_name)
                    else:
                        collections.add(collection_name)

            return list(collections)

        except Exception as e:
            print(f"❌ 获取collection列表失败: {e}")
            return []

    def get_shard_config(self) -> ShardConfig:
        """获取分片配置"""
        return self.shard_config

    def set_shard_config(self, config: ShardConfig):
        """设置分片配置"""
        self.shard_config = config

    def _connect(self) -> bool:
        """建立MongoDB连接"""
        try:
            if self.mongo.username and self.mongo.password:
                uri = f"mongodb://{self.mongo.username}:{self.mongo.password}@{self.mongo.host}:{self.mongo.port}/admin"
            else:
                uri = f"mongodb://{self.mongo.host}:{self.mongo.port}/"

            self.client = MongoClient(uri)
            return True
        except Exception as e:
            print(f"❌ MongoDB连接失败: {e}")
            return False

    def _disconnect(self):
        """断开MongoDB连接"""
        if self.client:
            self.client.close()

    def _read_shard_metadata(self, db_name: str, collection_name: str, db_dump_dir: str) -> Optional[Dict[str, Any]]:
        """读取分片元数据"""
        metadata_file = os.path.join(db_dump_dir, f"{collection_name}_shards.json")
        if not os.path.exists(metadata_file):
            return None

        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)

                # 验证分片文件是否都存在且不为空
                shard_count = metadata.get('shard_count', 0)
                for i in range(shard_count):
                    shard_file = os.path.join(db_dump_dir, f"{collection_name}_shard_{i:03d}.bson")
                    if not os.path.exists(shard_file):
                        print(f"⚠️ 分片文件缺失: {shard_file}")
                        return None

                    file_size = os.path.getsize(shard_file)
                    if file_size == 0:
                        print(f"⚠️ 分片文件为空: {shard_file} (大小: {file_size} 字节)")
                        return None

                return metadata
        except Exception as e:
            print(f"❌ 读取分片元数据失败 {metadata_file}: {e}")
            return None

    def _get_database_collections(self, database: str, dump_root_path: str) -> List[str]:
        """从dump目录获取数据库中的所有collection名称"""
        db_dump_dir = os.path.join(dump_root_path, database)
        if not os.path.exists(db_dump_dir):
            return []

        # 获取所有.bson文件（排除索引文件）
        collections = []
        for filename in os.listdir(db_dump_dir):
            if filename.endswith('.bson') and not filename.endswith('.metadata.json.bson'):
                collection_name = filename.replace('.bson', '')
                # 排除系统集合
                if collection_name.startswith('system.'):
                    continue

                # 如果是分片文件，提取原始collection名称
                if '_shard_' in collection_name:
                    original_name = collection_name.split('_shard_')[0]
                    collections.append(original_name)
                else:
                    collections.append(collection_name)

        return list(set(collections))  # 去重

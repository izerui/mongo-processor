#!/usr/bin/env python3
"""
数据库导出类 - 支持分片导出
集成了分片判断、范围计算和导出逻辑
"""
import math
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from typing import List

from bson import ObjectId

from base import MyMongo, MongoConfig, ObjectIdRange
from src.base import GlobalConfig


class MyDump(MyMongo):
    """
    按数据库整体导出，支持大collection分片
    """

    def __init__(self, mongo: MongoConfig, global_config: GlobalConfig):
        super().__init__(mongo, global_config)

    def export_db(self, database: str):
        """
        按数据库整体导出，支持大collection自动分片
        :param database: 数据库名
        :return: 导出的数据库目录路径
        """
        try:
            print(f"🚀 开始导出数据库: {database}")

            if self.global_config.enable_sharding:
                return self._export_db_with_sharding(database)
            else:
                return self._export_db_normal(database)

        except Exception as e:
            print(f'❌ 导出数据库 {database} 失败: {e}')
            raise

    def _export_db_with_sharding(self, database: str):
        """
        支持分片的数据库导出 - 使用快速统计信息优化判断
        """

        try:
            # 获取数据库中的所有collection
            collections = self.get_database_collections(database)
            if not collections:
                print(f"⚠️ 数据库 {database} 中没有collection")
                return os.path.join(self.global_config.dump_root_path, database)

            # 过滤忽略的集合
            filtered_collections = []
            ignored_collections = []
            for collection_name in collections:
                full_name = f"{database}.{collection_name}"
                if full_name in self.global_config.ignore_collections:
                    ignored_collections.append(collection_name)
                else:
                    filtered_collections.append(collection_name)

            collections = filtered_collections

            if ignored_collections:
                print(f"🚫 数据库 {database} 忽略 {len(ignored_collections)} 个集合: {ignored_collections}")

            if not collections:
                print(f"⚠️ 数据库 {database} 中没有需要导出的collection")
                return os.path.join(self.global_config.dump_root_path, database)

            print(f"📊 数据库 {database} 包含 {len(collections)} 个需要导出的collection")

            # 使用快速统计信息分析哪些collection需要分片
            large_collections = []
            small_collections = []

            # 批量获取所有集合的统计信息
            collection_counts = self.get_collection_counts_fast(database)

            for collection_name in collections:
                count = collection_counts.get(collection_name, 0)
                if count >= self.global_config.min_documents_for_shard:
                    large_collections.append(collection_name)
                else:
                    small_collections.append(collection_name)

            # 步骤1: 使用exclude参数导出所有非大集合
            if small_collections:
                print(f"📦 开始导出 {len(small_collections)} 个非大集合...")
                self._export_collections_with_exclude(database, large_collections)

            # 步骤2: 分片导出所有大集合（使用精确计数计算分片）
            if large_collections:
                print(f"🔄 开始分片导出 {len(large_collections)} 个大集合...")
                with ThreadPoolExecutor(max_workers=self.global_config.numParallelCollections) as executor:
                    futures = []
                    for collection_name in large_collections:
                        # 获取精确计数用于分片计算
                        db = self.client[database]
                        collection = db[collection_name]
                        exact_count = collection.count_documents({})

                        future = executor.submit(
                            self._export_collection_shards,
                            database, collection_name, exact_count
                        )
                        futures.append((future, collection_name))

                    for future, collection_name in futures:
                        try:
                            future.result()
                            print(f"✅ 大集合 {collection_name} 分片导出完成")
                        except Exception as e:
                            print(f"❌ 大集合 {collection_name} 分片导出失败: {e}")
                            raise

            # 数据库导出完成
            return os.path.join(self.global_config.dump_root_path, database)

        except Exception as e:
            print(f'❌ 分片导出数据库 {database} 失败: {e}')
            raise

    def _export_collections_with_exclude(self, database: str, exclude_collections: List[str]):
        """使用exclude参数导出除指定集合外的所有集合"""
        try:
            # 构建认证参数
            auth_append = ''
            if self.mongo_config.username and self.mongo_config.password:
                auth_append = f'--username={self.mongo_config.username} --password="{self.mongo_config.password}" --authenticationDatabase=admin'

            # 构建exclude参数
            exclude_params = ''
            if exclude_collections and len(exclude_collections) > 0:
                exclude_params = ' '.join([f'--excludeCollection={col}' for col in exclude_collections])

            # 构建导出命令 - 导出整个数据库但排除大集合
            export_cmd = (
                f'{self.mongodump_exe} '
                f'--host="{self.mongo_config.host}:{self.mongo_config.port}" '
                f'--db={database} '
                f'--out={self.global_config.dump_root_path} '
                f'--numParallelCollections={self.global_config.numParallelCollections} '
                f'{exclude_params} '
                f'{auth_append}'
            )

            print(f"📦 导出非大集合: 排除 {len(exclude_collections)} 个集合")
            self.exe_command(export_cmd, timeout=None)

        except Exception as e:
            print(f'❌ 导出非大集合失败: {e}')
            raise

    def _export_collection_shards(self, db_name: str, collection_name: str, exact_count: int = None):
        """分片导出单个collection，使用临时目录和重命名机制"""
        try:
            # 计算分片数量（使用精确计数）
            if exact_count is None:
                db = self.client[db_name]
                exact_count = db[collection_name].count_documents({})

            shard_count = self._calculate_optimal_shard_count(db_name, collection_name, exact_count)

            # 获取分片范围
            ranges = self._get_collection_objectid_ranges(db_name, collection_name, shard_count, exact_count)
            if not ranges:
                print(f"⚠️ 无法获取集合 {db_name}.{collection_name} 的分片范围，使用常规导出")
                return self._export_collection_normal(db_name, collection_name)

            print(f"🔄 开始分片导出 {db_name}.{collection_name}，分片数: {len(ranges)}，文档数: {exact_count:,}")

            # 创建分片导出目录结构: dumps/{database}/{collection}_partXXX/
            # db_dumps_dir = os.path.join(self.global_config.dump_root_path, db_name)
            # os.makedirs(db_dumps_dir, exist_ok=True)

            # 并发导出分片（并发度=分片数）
            with ThreadPoolExecutor(max_workers=len(ranges)) as executor:
                futures = []
                for i, obj_range in enumerate(ranges):
                    future = executor.submit(
                        self._export_single_shard,
                        db_name, collection_name, i, obj_range
                    )
                    futures.append(future)

                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        print(f"❌ 分片导出失败: {e}")
                        raise

            # 数据库目录
            db_dir = os.path.join(self.global_config.dump_root_path, db_name)
            os.makedirs(db_dir, exist_ok=True)

            # 保存分片元数据到数据库目录
            # self._save_shard_metadata(db_dir, db_name, collection_name, ranges)

            print(f"🎉 集合 {db_name}.{collection_name} 分片导出完成，共 {len(ranges)} 个分片")

        except Exception as e:
            print(f"❌ 分片导出集合 {db_name}.{collection_name} 失败: {e}")
            raise

    def _export_single_shard(self, db_name: str, collection_name: str, shard_idx: int, obj_range: ObjectIdRange):
        """导出单个分片到指定目录"""
        try:
            # 构建分片目录名和文件名
            part_suffix = f"_part{shard_idx + 1:03d}"
            collection_dir_name = f"{collection_name}{part_suffix}"
            part_dir = os.path.join(self.global_config.dump_root_path, db_name, collection_dir_name)
            os.makedirs(part_dir, exist_ok=True)

            # 构建查询条件
            query_dict = obj_range.to_query()

            # 构建认证参数
            auth_append = ''
            if self.mongo_config.username and self.mongo_config.password:
                auth_append = f'--username={self.mongo_config.username} --password="{self.mongo_config.password}" --authenticationDatabase=admin'

            # 构建导出命令 - 导出到分片目录
            export_cmd = (
                f'{self.mongodump_exe} '
                f'--host="{self.mongo_config.host}:{self.mongo_config.port}" '
                f'--db={db_name} '
                f'--collection={collection_name} '
                f'--out={part_dir} '
                f'{auth_append}'
            )

            # 修复查询条件格式
            if query_dict:
                # 使用正确的Extended JSON格式处理ObjectId
                query_parts = []
                if '_id' in query_dict:
                    id_query = query_dict['_id']
                    if '$gte' in id_query:
                        query_parts.append(f'"_id":{{"$gte":{{"$oid":"{str(id_query["$gte"])}"}}}}')
                    if '$lt' in id_query:
                        query_parts.append(f'"_id":{{"$lt":{{"$oid":"{str(id_query["$lt"])}"}}}}')

                if query_parts:
                    query_str = "{" + ",".join(query_parts) + "}"
                    export_cmd += f' --query=\'{query_str}\''

            # 执行分片导出
            self.exe_command(export_cmd, timeout=None)

            # 验证分片目录中的导出结果
            collection_bson = os.path.join(part_dir, db_name, f"{collection_name}.bson")
            collection_metadata = os.path.join(part_dir, db_name, f"{collection_name}.metadata.json")

            # 验证文件是否存在且不为空
            if not os.path.exists(collection_bson):
                print(f"❌ 分片导出失败: 文件不存在 {collection_bson}")
                # 检查分片目录内容
                if os.path.exists(part_dir):
                    files = os.listdir(part_dir)
                    print(f"📁 分片目录内容: {files}")
                raise Exception(f"分片导出失败: 文件不存在 {collection_bson}")

            # 构建目标路径（移动到外层 os.path.join(dump_root_path, db_name)）
            target_dir = os.path.join(self.global_config.dump_root_path, db_name)
            target_bson = os.path.join(target_dir, f"{collection_name}{part_suffix}.bson")
            target_metadata = os.path.join(target_dir, f"{collection_name}{part_suffix}.metadata.json")

            # 重命名并移动文件
            if os.path.exists(collection_bson):
                shutil.move(collection_bson, target_bson)
            if os.path.exists(collection_metadata):
                shutil.move(collection_metadata, target_metadata)

            # 删除 part_dir
            if os.path.exists(part_dir):
                shutil.rmtree(part_dir)

            print(f"✅ 分片 {shard_idx + 1} 导出完成，文件已移动到: {target_dir}")

        except Exception as e:
            print(f"❌ 导出分片 {shard_idx + 1} 失败: {e}")
            raise

    def _export_collection_normal(self, db_name: str, collection_name: str):
        """常规导出单个collection（不分片）"""
        try:
            # 构建认证参数
            auth_append = ''
            if self.mongo_config.username and self.mongo_config.password:
                auth_append = f'--username={self.mongo_config.username} --password="{self.mongo_config.password}" --authenticationDatabase=admin'

            # 构建导出命令
            export_cmd = (
                f'{self.mongodump_exe} '
                f'--host="{self.mongo_config.host}:{self.mongo_config.port}" '
                f'--db={db_name} '
                f'--collection={collection_name} '
                f'--out={self.global_config.dump_root_path} '
                f'{auth_append}'
            )

            # 执行导出
            self.exe_command(export_cmd, timeout=None)

            # 验证文件是否存在且不为空
            output_bson = os.path.join(self.global_config.dump_root_path, db_name, f"{collection_name}.bson")
            if not os.path.exists(output_bson):
                raise Exception(f"常规导出失败: 文件不存在 {output_bson}")

            file_size = os.path.getsize(output_bson)
            if file_size == 0:
                raise Exception(f"常规导出失败: 文件为空 {output_bson} (大小: {file_size} 字节)")

            print(f"✅ 常规导出成功: {output_bson} (大小: {file_size} 字节)")

        except Exception as e:
            print(f"❌ 常规导出集合 {db_name}.{collection_name} 失败: {e}")
            raise

    def _export_db_normal(self, database: str):
        """常规数据库导出（不使用分片）"""
        try:
            print(f"📦 开始常规导出数据库: {database}")

            # 获取需要忽略的集合
            ignore_collections_for_db = []
            for ignore_pattern in self.global_config.ignore_collections:
                if ignore_pattern.startswith(f"{database}."):
                    collection_name = ignore_pattern[len(database)+1:]
                    ignore_collections_for_db.append(collection_name)

            if ignore_collections_for_db:
                print(f"🚫 忽略集合: {ignore_collections_for_db}")

            # 构建认证参数
            auth_append = ''
            if self.mongo_config.username and self.mongo_config.password:
                auth_append = f'--username={self.mongo_config.username} --password="{self.mongo_config.password}" --authenticationDatabase=admin'
            pass
            # 构建导出命令 - 直接导出到dumps/{database}/
            exclude_params = ''
            if ignore_collections_for_db:
                exclude_params = ' '.join([f'--excludeCollection={col}' for col in ignore_collections_for_db])

            export_cmd = (
                f'{self.mongodump_exe} '
                f'--host="{self.mongo_config.host}:{self.mongo_config.port}" '
                f'--db={database} '
                f'--out={self.global_config.dump_root_path} '
                f'{exclude_params} '
                f'{auth_append}'
            )

            # 执行导出
            self.exe_command(export_cmd, timeout=None)

            # 验证数据库目录是否存在且包含文件
            db_dir = os.path.join(self.global_config.dump_root_path, database)
            if not os.path.exists(db_dir):
                raise Exception(f"数据库导出失败: 目录不存在 {db_dir}")

            # 检查是否有有效的导出文件
            has_files = False
            for filename in os.listdir(db_dir):
                if filename.endswith('.bson'):
                    file_path = os.path.join(db_dir, filename)
                    file_size = os.path.getsize(file_path)
                    if file_size > 0:
                        has_files = True
                        print(f"✅ 数据库导出包含: {filename} (大小: {file_size} 字节)")

            if not has_files:
                raise Exception(f"数据库导出失败: 没有找到有效的导出文件")

            # 数据库导出完成
            return db_dir

        except Exception as e:
            print(f'❌ 常规导出数据库 {database} 失败: {e}')
            raise

    def _calculate_optimal_shard_count(self, database: str, collection_name: str, doc_count: int = None) -> int:
        """计算最优分片数量（使用精确计数）"""
        try:
            if doc_count is None:
                db = self.client[database]
                collection = db[collection_name]
                doc_count = collection.count_documents({})

            # 根据文档数量计算分片数
            if doc_count < self.global_config.min_documents_for_shard:
                return 1

            # 计算需要的分片数（每100万文档一个分片）
            needed_shards = max(1, min(
                math.ceil(doc_count / self.global_config.min_documents_for_shard),
                self.global_config.max_shard_count
            ))

            print(
                f"📊 分片计算: {doc_count:,} 条记录 -> {needed_shards} 个分片 (最大: {self.global_config.max_shard_count})")

            return needed_shards

        except Exception as e:
            print(f"⚠️ 计算分片数量失败: {e}")
            return self.global_config.default_shard_count

    def _get_collection_objectid_ranges(self, db_name: str, collection_name: str, shard_count: int,
                                        doc_count: int = None) -> List[
        ObjectIdRange]:
        """获取collection的ObjectId分片范围"""
        try:
            if shard_count <= 1:
                return []

            db = self.client[db_name]
            collection = db[collection_name]

            # 如果提供了精确计数且分片数为1，直接返回空列表
            if doc_count is not None and shard_count <= 1:
                return []

            # 获取最小和最大的ObjectId
            min_doc = collection.find_one(sort=[("_id", 1)])
            max_doc = collection.find_one(sort=[("_id", -1)])

            if not min_doc or not max_doc:
                return []

            min_id = min_doc["_id"]
            max_id = max_doc["_id"]

            # 如果最小和最大ID相同，不需要分片
            if min_id == max_id:
                return []

            # 计算每个分片的范围
            ranges = []

            # 检查ObjectId类型
            try:
                min_str = str(min_id)
                max_str = str(max_id)

                # ObjectId字符串格式: 24个十六进制字符
                if len(min_str) != 24 or len(max_str) != 24:
                    return []

                min_int = int(min_str, 16)
                max_int = int(max_str, 16)
                total_range = max_int - min_int

                if total_range <= 0:
                    return []

            except ValueError as e:
                print(f"⚠️ 分片调试: ObjectId转换失败: {e}")
                return []

            shard_size = total_range // shard_count

            for i in range(shard_count):
                start_offset = i * shard_size
                end_offset = (i + 1) * shard_size if i < shard_count - 1 else total_range

                start_hex = hex(min_int + start_offset)[2:].zfill(24)
                end_hex = hex(min_int + end_offset)[2:].zfill(24)

                start_id = ObjectId(start_hex)
                if i == shard_count - 1:
                    end_id = None
                else:
                    end_id = ObjectId(end_hex)
                ranges.append(ObjectIdRange(start_id, end_id))

            return ranges

        except Exception as e:
            return []

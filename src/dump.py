#!/usr/bin/env python3
"""
数据库导出类 - 支持分片导出
集成了分片判断、范围计算和导出逻辑
"""
import os
import json
import math
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient
from pymongo.collection import Collection
from bson import ObjectId
import shutil

from base import Shell, Mongo, mongodump_exe, ObjectIdRange

from base import ShardConfig


class MyDump(Shell):
    """
    按数据库整体导出，支持大collection分片
    """

    def __init__(self, mongo: Mongo, numParallelCollections: int = 4,
                 enable_sharding: bool = True, shard_config: Optional[ShardConfig] = None):
        super().__init__()
        self.mongo = mongo
        self.numParallelCollections = numParallelCollections
        self.enable_sharding = enable_sharding
        self.shard_config = shard_config or ShardConfig()
        self.client = None

    def export_db(self, database: str, dump_root_path: str):
        """
        按数据库整体导出，支持大collection自动分片
        :param database: 数据库名
        :param dump_root_path: 导出根目录
        :return: 导出的数据库目录路径
        """
        try:
            print(f"🚀 开始导出数据库: {database}")

            if self.enable_sharding:
                return self._export_db_with_sharding(database, dump_root_path)
            else:
                return self._export_db_normal(database, dump_root_path)

        except Exception as e:
            print(f'❌ 导出数据库 {database} 失败: {e}')
            raise

    def _export_db_with_sharding(self, database: str, dump_root_path: str):
        """
        支持分片的数据库导出 - 使用临时目录和重命名机制
        """
        import shutil

        try:
            # 获取数据库中的所有collection
            collections = self._get_database_collections(database)
            if not collections:
                print(f"⚠️ 数据库 {database} 中没有collection")
                return os.path.join(dump_root_path, database)

            print(f"📊 数据库 {database} 包含 {len(collections)} 个collection")

            # 分析哪些collection需要分片
            large_collections = []
            small_collections = []

            for collection_name in collections:
                if self._should_shard_collection(database, collection_name):
                    large_collections.append(collection_name)
                    print(f"🔄 Collection {collection_name} 将使用分片导出")
                else:
                    small_collections.append(collection_name)
                    print(f"📦 Collection {collection_name} 使用常规导出")

            # 步骤1: 使用exclude参数导出所有非大集合
            if small_collections:
                print(f"📦 开始导出 {len(small_collections)} 个非大集合...")
                self._export_collections_with_exclude(database, large_collections, dump_root_path)

            # 步骤2: 分片导出所有大集合
            if large_collections:
                print(f"🔄 开始分片导出 {len(large_collections)} 个大集合...")
                with ThreadPoolExecutor(max_workers=self.numParallelCollections) as executor:
                    futures = []
                    for collection_name in large_collections:
                        future = executor.submit(
                            self._export_collection_shards,
                            database, collection_name, dump_root_path
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
            return os.path.join(dump_root_path, database)

        except Exception as e:
            print(f'❌ 分片导出数据库 {database} 失败: {e}')
            raise
        finally:
            self._disconnect()

    def _export_collections_with_exclude(self, database: str, exclude_collections: List[str], dump_root_path: str):
        """使用exclude参数导出除指定集合外的所有集合"""
        try:
            # 构建认证参数
            auth_append = ''
            if self.mongo.username and self.mongo.password:
                auth_append = f'--username={self.mongo.username} --password="{self.mongo.password}" --authenticationDatabase=admin'
                # 构建exclude参数
                exclude_params = ''
                if exclude_collections and len(exclude_collections) > 0:
                    exclude_params = ' '.join([f'--excludeCollection={col}' for col in exclude_collections])

            # 构建导出命令 - 导出整个数据库但排除大集合
            export_cmd = (
                f'{mongodump_exe} '
                f'--host="{self.mongo.host}:{self.mongo.port}" '
                f'--db={database} '
                f'--out={dump_root_path} '
                f'{exclude_params} '
                f'{auth_append}'
            )

            print(f"📦 导出非大集合: 排除 {len(exclude_collections)} 个集合")
            self._exe_command(export_cmd)

        except Exception as e:
            print(f'❌ 导出非大集合失败: {e}')
            raise

    def _export_collection_shards(self, db_name: str, collection_name: str, dump_root_path: str):
        """分片导出单个collection，使用临时目录和重命名机制"""
        try:
            # 计算分片数量
            shard_count = self._calculate_optimal_shard_count(db_name, collection_name)

            # 获取分片范围
            ranges = self._get_collection_objectid_ranges(db_name, collection_name, shard_count)
            if not ranges:
                print(f"⚠️ 无法获取集合 {db_name}.{collection_name} 的分片范围，使用常规导出")
                return self._export_collection_normal(db_name, collection_name, dump_root_path)

            print(f"🔄 开始分片导出 {db_name}.{collection_name}，分片数: {len(ranges)}")

            # 创建分片导出目录结构: dumps/{database}/{collection}_partXXX/
            dumps_dir = os.path.join(dump_root_path, db_name)
            os.makedirs(dumps_dir, exist_ok=True)

            # 并发导出分片（并发度=分片数）
            with ThreadPoolExecutor(max_workers=len(ranges)) as executor:
                futures = []
                for i, obj_range in enumerate(ranges):
                    future = executor.submit(
                        self._export_single_shard,
                        db_name, collection_name, dump_root_path, i, obj_range
                    )
                    futures.append(future)

                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        print(f"❌ 分片导出失败: {e}")
                        raise

            # 数据库目录
            db_dir = os.path.join(dump_root_path, 'dumps', db_name)
            os.makedirs(db_dir, exist_ok=True)

            # 保存分片元数据到数据库目录
            # self._save_shard_metadata(db_dir, db_name, collection_name, ranges)

            print(f"🎉 集合 {db_name}.{collection_name} 分片导出完成，共 {len(ranges)} 个分片")

        except Exception as e:
            print(f"❌ 分片导出集合 {db_name}.{collection_name} 失败: {e}")
            raise

    def _export_single_shard(self, db_name: str, collection_name: str,
                             dump_root_path: str, shard_idx: int, obj_range: ObjectIdRange):
        """导出单个分片到指定目录"""
        try:
            # 构建分片目录名和文件名
            part_suffix = f"_part{shard_idx + 1:03d}"
            collection_dir_name = f"{collection_name}{part_suffix}"
            part_dir = os.path.join(dump_root_path, db_name, collection_dir_name)
            os.makedirs(part_dir, exist_ok=True)

            # 构建查询条件
            query_dict = obj_range.to_query()
            query_json = json.dumps(query_dict, default=str) if query_dict else "{}"

            # 构建认证参数
            auth_append = ''
            if self.mongo.username and self.mongo.password:
                auth_append = f'--username={self.mongo.username} --password="{self.mongo.password}" --authenticationDatabase=admin'

            # 构建导出命令 - 导出到分片目录
            export_cmd = (
                f'{mongodump_exe} '
                f'--host="{self.mongo.host}:{self.mongo.port}" '
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
            self._exe_command(export_cmd)

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
            target_dir = os.path.join(dump_root_path, db_name)
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

            # 修改分片元数据，将集合名改为当前分片的文件名
            part_suffix = f"_part{shard_idx + 1:03d}"
            shard_collection_name = f"{collection_name}{part_suffix}"
            metadata_file = os.path.join(target_dir, f"{shard_collection_name}.metadata.json")

            if os.path.exists(metadata_file):
                try:
                    with open(metadata_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)

                    # 修改集合名称为分片文件名
                    metadata['collectionName'] = shard_collection_name

                    # 修改indexes中的ns字段
                    if 'indexes' in metadata:
                        for index in metadata['indexes']:
                            if 'ns' in index:
                                index['ns'] = f"{db_name}.{shard_collection_name}"

                    with open(metadata_file, 'w', encoding='utf-8') as f:
                        json.dump(metadata, f, indent=2, ensure_ascii=False)

                    print(f"📝 已修改分片元数据: {metadata_file}，集合名改为: {shard_collection_name}")
                except Exception as e:
                    print(f"⚠️ 修改分片元数据失败: {e}")

            print(f"✅ 分片 {shard_idx + 1} 导出完成，文件已移动到: {target_dir}")

        except Exception as e:
            print(f"❌ 导出分片 {shard_idx + 1} 失败: {e}")
            raise

    def _export_collection_normal(self, db_name: str, collection_name: str, dump_root_path: str):
        """常规导出单个collection（不分片）"""
        try:
            # 构建认证参数
            auth_append = ''
            if self.mongo.username and self.mongo.password:
                auth_append = f'--username={self.mongo.username} --password="{self.mongo.password}" --authenticationDatabase=admin'

            # 构建导出命令
            export_cmd = (
                f'{mongodump_exe} '
                f'--host="{self.mongo.host}:{self.mongo.port}" '
                f'--db={db_name} '
                f'--collection={collection_name} '
                f'--out={dump_root_path} '
                f'{auth_append}'
            )

            # 执行导出
            self._exe_command(export_cmd)

            # 验证文件是否存在且不为空
            output_bson = os.path.join(dump_root_path, db_name, f"{collection_name}.bson")
            if not os.path.exists(output_bson):
                raise Exception(f"常规导出失败: 文件不存在 {output_bson}")

            file_size = os.path.getsize(output_bson)
            if file_size == 0:
                raise Exception(f"常规导出失败: 文件为空 {output_bson} (大小: {file_size} 字节)")

            print(f"✅ 常规导出成功: {output_bson} (大小: {file_size} 字节)")

        except Exception as e:
            print(f"❌ 常规导出集合 {db_name}.{collection_name} 失败: {e}")
            raise

    def _save_shard_metadata(self, output_dir: str, db_name: str, collection_name: str, ranges: List[ObjectIdRange]):
        """保存分片元数据"""
        try:
            metadata = {
                "collection": collection_name,
                "shard_count": len(ranges),
                "created_at": datetime.now().isoformat(),
                "ranges": [
                    {
                        "shard_index": i,
                        "start_id": str(r.start_id) if r.start_id else None,
                        "end_id": str(r.end_id) if r.end_id else None
                    }
                    for i, r in enumerate(ranges)
                ]
            }

            db_dir = os.path.join(output_dir, db_name)
            metadata_file = os.path.join(db_dir, f"{collection_name}_shards.json")
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            print(f"💾 已保存分片元数据: {metadata_file}")

        except Exception as e:
            print(f"⚠️ 保存分片元数据失败: {e}")

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

    def _get_database_collections(self, database: str) -> List[str]:
        """获取数据库中的所有collection名称"""
        try:
            if not self._connect():
                return []

            db = self.client[database]

            # 获取所有collection名称，排除系统collection
            collections = [name for name in db.list_collection_names()
                           if not name.startswith('system.')]

            return collections

        except Exception as e:
            print(f"❌ 获取数据库 {database} collection列表失败: {e}")
            return []

    def _export_db_normal(self, database: str, dump_root_path: str):
        """常规数据库导出（不使用分片）"""
        try:
            print(f"📦 开始常规导出数据库: {database}")

            # 构建认证参数
            auth_append = ''
            if self.mongo.username and self.mongo.password:
                auth_append = f'--username={self.mongo.username} --password="{self.mongo.password}" --authenticationDatabase=admin'

            # 构建导出命令 - 直接导出到dumps/{database}/
            export_cmd = (
                f'{mongodump_exe} '
                f'--host="{self.mongo.host}:{self.mongo.port}" '
                f'--db={database} '
                f'--out={dump_root_path} '
                f'{auth_append}'
            )

            # 执行导出
            self._exe_command(export_cmd)

            # 验证数据库目录是否存在且包含文件
            db_dir = os.path.join(dump_root_path, database)
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

    def _should_shard_collection(self, database: str, collection_name: str) -> bool:
        """判断collection是否需要分片导出"""
        try:
            if not self._connect():
                return False

            db = self.client[database]
            collection = db[collection_name]

            # 获取文档数量
            doc_count = collection.estimated_document_count()

            # 只有大集合才显示条目数
            is_large = doc_count >= self.shard_config.min_documents_for_shard
            if is_large:
                print(f"📊 大集合 {database}.{collection_name}: {doc_count:,} 条记录")

            return is_large

        except Exception as e:
            print(f"⚠️ 判断collection {database}.{collection_name} 分片需求失败: {e}")
            return False

    def _calculate_optimal_shard_count(self, db_name: str, collection_name: str) -> int:
        """计算最优的分片数量"""
        try:
            if not self._connect():
                return 1

            db = self.client[db_name]
            collection = db[collection_name]

            # 获取文档数量
            doc_count = collection.estimated_document_count()

            # 根据文档数量计算分片数
            if doc_count < self.shard_config.min_documents_for_shard:
                return 1

            # 计算需要的分片数（每100万文档一个分片）
            needed_shards = max(1, min(
                math.ceil(doc_count / self.shard_config.min_documents_for_shard),
                self.shard_config.max_shard_count
            ))

            print(
                f"📊 分片计算: {doc_count:,} 条记录 -> {needed_shards} 个分片 (最大: {self.shard_config.max_shard_count})")

            return needed_shards

        except Exception as e:
            print(f"⚠️ 计算分片数量失败: {e}")
            return self.shard_config.default_shard_count

    def _get_collection_objectid_ranges(self, db_name: str, collection_name: str, shard_count: int) -> List[
        ObjectIdRange]:
        """获取collection的ObjectId分片范围"""
        try:
            if not self._connect() or shard_count <= 1:
                return []

            db = self.client[db_name]
            collection = db[collection_name]

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

    def get_shard_config(self) -> ShardConfig:
        """获取分片配置"""
        return self.shard_config

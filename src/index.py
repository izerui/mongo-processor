#!/usr/bin/env python3
"""
索引处理模块
用于从source MongoDB读取索引信息，并在target MongoDB后台创建索引
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional
from pymongo import MongoClient, IndexModel
from pymongo.errors import OperationFailure, DuplicateKeyError
import time

logger = logging.getLogger(__name__)


class IndexManager:
    """索引管理器，负责读取和创建索引"""

    def __init__(self, source_client: MongoClient, target_client: MongoClient):
        """
        初始化索引管理器

        Args:
            source_client: 源MongoDB客户端
            target_client: 目标MongoDB客户端
        """
        self.source_client = source_client
        self.target_client = target_client

    def get_database_indexes(self, database_name: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        获取指定数据库的所有集合的索引信息

        Args:
            database_name: 数据库名称

        Returns:
            字典，键为集合名，值为索引列表
        """
        indexes_info = {}

        try:
            db = self.source_client[database_name]

            # 获取数据库中的所有集合
            collections = db.list_collection_names()

            for collection_name in collections:
                try:
                    # 获取集合的索引信息
                    indexes = db[collection_name].list_indexes()
                    indexes_info[collection_name] = []

                    for index in indexes:
                        # 跳过默认的_id索引
                        if index['name'] == '_id_':
                            continue

                        # 构建索引信息
                        index_info = {
                            'name': index['name'],
                            'key': index['key'],
                            'unique': index.get('unique', False),
                            'sparse': index.get('sparse', False),
                            'background': True,  # 强制后台创建
                            'expireAfterSeconds': index.get('expireAfterSeconds'),
                            'partialFilterExpression': index.get('partialFilterExpression'),
                            'collation': index.get('collation')
                        }

                        # 移除None值
                        index_info = {k: v for k, v in index_info.items() if v is not None}
                        indexes_info[collection_name].append(index_info)

                except OperationFailure as e:
                    logger.warning(f"获取集合 {collection_name} 的索引失败: {e}")
                    continue

        except Exception as e:
            logger.error(f"获取数据库 {database_name} 的索引信息失败: {e}")

        return indexes_info

    def create_indexes_on_target(self, database_name: str, indexes_info: Dict[str, List[Dict[str, Any]]]) -> Dict[str, bool]:
        """
        在目标数据库上创建索引

        Args:
            database_name: 数据库名称
            indexes_info: 索引信息字典

        Returns:
            字典，键为集合名，值为是否成功
        """
        results = {}

        for collection_name, indexes in indexes_info.items():
            try:
                target_db = self.target_client[database_name]
                target_collection = target_db[collection_name]

                if not indexes:
                    results[collection_name] = True
                    continue

                # 准备索引模型
                index_models = []
                for index_info in indexes:
                    # 提取索引键
                    keys = list(index_info['key'].items())

                    # 创建索引选项
                    options = {
                        'name': index_info['name'],
                        'background': True,  # 强制后台创建
                    }

                    # 添加其他选项
                    if 'unique' in index_info:
                        options['unique'] = index_info['unique']
                    if 'sparse' in index_info:
                        options['sparse'] = index_info['sparse']
                    if 'expireAfterSeconds' in index_info:
                        options['expireAfterSeconds'] = index_info['expireAfterSeconds']
                    if 'partialFilterExpression' in index_info:
                        options['partialFilterExpression'] = index_info['partialFilterExpression']
                    if 'collation' in index_info:
                        options['collation'] = index_info['collation']

                    index_model = IndexModel(keys, **options)
                    index_models.append(index_model)

                # 批量创建索引
                if index_models:
                    target_collection.create_indexes(index_models)

                results[collection_name] = True
                logger.info(f"成功在集合 {collection_name} 上创建 {len(indexes)} 个索引")

            except DuplicateKeyError as e:
                logger.warning(f"集合 {collection_name} 索引创建时存在重复键: {e}")
                results[collection_name] = True  # 视为成功，因为索引已存在
            except OperationFailure as e:
                logger.error(f"集合 {collection_name} 索引创建失败: {e}")
                results[collection_name] = False
            except Exception as e:
                logger.error(f"集合 {collection_name} 索引创建时发生未知错误: {e}")
                results[collection_name] = False

        return results

    def recreate_indexes_for_database(self, database_name: str) -> Dict[str, Any]:
        """
        重新创建单个数据库的所有索引

        Args:
            database_name: 数据库名称

        Returns:
            包含统计信息的字典
        """
        start_time = time.time()

        try:
            # 获取源数据库的索引信息
            indexes_info = self.get_database_indexes(database_name)

            if not indexes_info:
                return {
                    'database': database_name,
                    'success': True,
                    'total_indexes': 0,
                    'created_indexes': 0,
                    'failed_collections': [],
                    'duration': time.time() - start_time
                }

            # 计算总索引数
            total_indexes = sum(len(indexes) for indexes in indexes_info.values())

            if total_indexes == 0:
                return {
                    'database': database_name,
                    'success': True,
                    'total_indexes': 0,
                    'created_indexes': 0,
                    'failed_collections': [],
                    'duration': time.time() - start_time
                }

            # 在目标数据库上创建索引
            results = self.create_indexes_on_target(database_name, indexes_info)

            # 统计失败的集合
            failed_collections = [col for col, success in results.items() if not success]
            created_indexes = total_indexes - len(failed_collections)

            return {
                'database': database_name,
                'success': len(failed_collections) == 0,
                'total_indexes': total_indexes,
                'created_indexes': created_indexes,
                'failed_collections': failed_collections,
                'duration': time.time() - start_time
            }

        except Exception as e:
            logger.error(f"重新创建数据库 {database_name} 的索引时发生错误: {e}")
            return {
                'database': database_name,
                'success': False,
                'total_indexes': 0,
                'created_indexes': 0,
                'failed_collections': [],
                'duration': time.time() - start_time,
                'error': str(e)
            }

    def recreate_indexes_for_databases(self, database_names: List[str], max_workers: int = 4) -> Dict[str, Any]:
        """
        并发重新创建多个数据库的索引

        Args:
            database_names: 数据库名称列表
            max_workers: 最大并发线程数

        Returns:
            包含总体统计信息的字典
        """
        start_time = time.time()
        total_databases = len(database_names)

        if total_databases == 0:
            return {
                'total_databases': 0,
                'successful_databases': 0,
                'failed_databases': [],
                'total_indexes': 0,
                'created_indexes': 0,
                'duration': 0
            }

        results = []

        # 使用线程池并发处理
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_db = {
                executor.submit(self.recreate_indexes_for_database, db_name): db_name
                for db_name in database_names
            }

            for future in as_completed(future_to_db):
                db_name = future_to_db[future]
                try:
                    result = future.result()
                    results.append(result)

                    if result['success']:
                        logger.info(f"✅ 数据库 {db_name} 索引创建完成 "
                                  f"(总索引: {result['total_indexes']}, "
                                  f"成功: {result['created_indexes']}, "
                                  f"耗时: {result['duration']:.2f}秒)")
                    else:
                        logger.warning(f"⚠️  数据库 {db_name} 索引创建失败 "
                                     f"(失败集合: {result.get('failed_collections', [])})")

                except Exception as e:
                    logger.error(f"处理数据库 {db_name} 时发生异常: {e}")
                    results.append({
                        'database': db_name,
                        'success': False,
                        'total_indexes': 0,
                        'created_indexes': 0,
                        'failed_collections': [],
                        'duration': 0,
                        'error': str(e)
                    })

        # 汇总统计
        successful_databases = [r for r in results if r['success']]
        failed_databases = [r for r in results if not r['success']]

        total_indexes = sum(r['total_indexes'] for r in results)
        created_indexes = sum(r['created_indexes'] for r in results)

        return {
            'total_databases': total_databases,
            'successful_databases': len(successful_databases),
            'failed_databases': failed_databases,
            'total_indexes': total_indexes,
            'created_indexes': created_indexes,
            'duration': time.time() - start_time,
            'details': results
        }


def create_mongo_client(host: str, port: int, username: str = None, password: str = None) -> MongoClient:
    """
    创建MongoDB客户端连接

    Args:
        host: MongoDB主机地址
        port: MongoDB端口
        username: 用户名（可选）
        password: 密码（可选）

    Returns:
        MongoClient实例
    """
    connection_string = f"mongodb://"

    if username and password:
        connection_string += f"{username}:{password}@"

    connection_string += f"{host}:{port}"

    return MongoClient(connection_string)

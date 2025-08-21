#!/usr/bin/env python3
"""
索引处理模块
用于从source MongoDB读取索引信息，并在target MongoDB后台创建索引
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any

from src.base import MyMongo

logger = logging.getLogger(__name__)


class IndexManager:
    """索引管理器，负责读取和创建索引"""

    def __init__(self, source_mongo: MyMongo, target_mongo: MyMongo):
        """
        初始化索引管理器

        Args:
            source_mongo: 源MongoDB客户端
            target_mongo: 目标MongoDB客户端
        """
        self.source_mongo = source_mongo
        self.target_mongo = target_mongo

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
            indexes_info = self.source_mongo.get_database_indexes(database_name)

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
            results = self.target_mongo.create_indexes_on_target(database_name, indexes_info)

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
            print(f"❌ 数据库 {database_name} 索引创建失败: {e}")
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
                        print(f"✅ 数据库 {db_name} 索引创建完成 "
                              f"(总索引: {result['total_indexes']}, "
                              f"成功: {result['created_indexes']}, "
                              f"耗时: {result['duration']:.2f}秒)")
                    else:
                        print(f"⚠️  数据库 {db_name} 索引创建失败 "
                              f"(失败集合: {result.get('failed_collections', [])})")

                except Exception as e:
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

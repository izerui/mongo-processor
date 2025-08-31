#!/usr/bin/env python3
"""
MongoDB 数据对比脚本

该脚本用于对比源数据库和目标数据库的数据差异，包括：
- 数据库和集合的存在性检查
- 文档数量对比
- 数据内容哈希对比
- 索引对比
- 生成详细的对比报告

使用方法:
    python compare_data.py
"""

import hashlib
import json
import sys

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

# 添加当前目录到Python路径
sys.path.insert(0, str(Path(__file__).parent))

from base import MyMongo, MongoConfig, GlobalConfig


class DataComparator:
    """数据对比器类"""

    def __init__(self):
        """初始化数据对比器"""
        self.config_path = Path(__file__).parent.parent / 'config.ini'

        self.global_config = None
        self.source_mongo = None
        self.target_mongo = None
        self._load_config()

        # 对比结果缓存
        self.comparison_cache = {}

    def _load_config(self):
        """加载配置文件"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")

        from configparser import ConfigParser
        config = ConfigParser()
        config.read(self.config_path)

        # 源数据库配置
        source_config = MongoConfig(
            config.get('source', 'host'),
            int(config.get('source', 'port')),
            config.get('source', 'username'),
            config.get('source', 'password')
        )

        # 目标数据库配置
        target_config = MongoConfig(
            config.get('target', 'host'),
            int(config.get('target', 'port')),
            config.get('target', 'username'),
            config.get('target', 'password')
        )

        # 全局配置
        self.global_config = GlobalConfig()
        self.global_config.databases = config.get('global', 'databases').split(',')
        self.global_config.ignore_collections = [
            col.strip()
            for col in config.get('global', 'ignoreCollections', fallback='').split(',')
            if col.strip()
        ]

        # 对比配置默认值
        self.sample_size = 1000  # 采样大小
        self.max_workers = 4     # 并发线程数
        self.detailed = False    # 详细对比模式
        self.timeout = 300       # 超时时间

        # 初始化MongoDB连接
        self.source_mongo = MyMongo(source_config, self.global_config)
        self.target_mongo = MyMongo(target_config, self.global_config)

    def get_databases(self) -> List[str]:
        """获取需要对比的数据库列表"""
        if not self.global_config or not self.global_config.databases:
            return []
        return [db.strip() for db in self.global_config.databases if db.strip()]

    def _get_collection_stats(self, mongo: MyMongo, database: str, collection: str) -> Dict[str, Any]:
        """获取集合统计信息"""
        try:
            db = mongo.client[database]
            stats = db.command('collStats', collection)
            return {
                'count': stats.get('count', 0),
                'size': stats.get('size', 0),
                'avgObjSize': stats.get('avgObjSize', 0),
                'storageSize': stats.get('storageSize', 0),
                'nindexes': stats.get('nindexes', 0)
            }
        except Exception as e:
            return {'error': str(e)}

    def _get_collection_indexes(self, mongo: MyMongo, database: str, collection: str) -> List[Dict[str, Any]]:
        """获取集合索引信息"""
        try:
            db = mongo.client[database]
            indexes = list(db[collection].list_indexes())
            # 排除默认的_id索引
            return [idx for idx in indexes if idx['name'] != '_id_']
        except Exception:
            return []

    def _calculate_collection_hash(self, mongo: MyMongo, database: str, collection: str,
                                 sample_size: int = 1000) -> str:
        """计算集合数据的哈希值（用于快速对比）"""
        try:
            db = mongo.client[database]
            coll = db[collection]

            # 获取文档总数
            total_docs = coll.count_documents({})
            if total_docs == 0:
                return "empty"

            # 使用提供的采样大小
            if sample_size <= 0:
                sample_size = self.sample_size

            # 采样文档进行哈希计算
            sample_docs = list(coll.find({}, {'_id': 1}).limit(sample_size))

            # 计算哈希
            hasher = hashlib.md5()
            for doc in sorted(sample_docs, key=lambda x: str(x.get('_id', ''))):
                doc_str = json.dumps(doc, sort_keys=True, default=str)
                hasher.update(doc_str.encode('utf-8'))

            return f"{hasher.hexdigest()}_{total_docs}_{sample_size}"

        except Exception as e:
            return f"error_{str(e)}"

    def _compare_collection_data(self, database: str, collection: str,
                               detailed: bool = False) -> Dict[str, Any]:
        """对比单个集合的数据"""
        result = {
            'collection': collection,
            'database': database,
            'exists_in_source': False,
            'exists_in_target': False,
            'source_stats': {},
            'target_stats': {},
            'count_diff': 0,
            'size_diff': 0,
            'index_diff': [],
            'data_hash_match': False,
            'issues': []
        }

        # 检查集合是否存在
        try:
            if not self.source_mongo or not self.target_mongo:
                result['issues'].append("MongoDB连接未初始化")
                return result

            source_collections = self.source_mongo.client[database].list_collection_names()
            target_collections = self.target_mongo.client[database].list_collection_names()

            result['exists_in_source'] = collection in source_collections
            result['exists_in_target'] = collection in target_collections

            if not result['exists_in_source']:
                result['issues'].append("集合在源数据库中不存在")
                return result

            if not result['exists_in_target']:
                result['issues'].append("集合在目标数据库中不存在")
                return result

            # 获取统计信息
            if not self.source_mongo or not self.target_mongo:
                result['issues'].append("MongoDB连接未初始化")
                return result

            result['source_stats'] = self._get_collection_stats(self.source_mongo, database, collection)
            result['target_stats'] = self._get_collection_stats(self.target_mongo, database, collection)

            # 计算差异
            if 'count' in result['source_stats'] and 'count' in result['target_stats']:
                result['count_diff'] = result['source_stats']['count'] - result['target_stats']['count']
                result['source_count'] = result['source_stats']['count']
                result['target_count'] = result['target_stats']['count']

            if 'size' in result['source_stats'] and 'size' in result['target_stats']:
                result['size_diff'] = result['source_stats']['size'] - result['target_stats']['size']

            # 对比索引
            if not self.source_mongo or not self.target_mongo:
                result['issues'].append("MongoDB连接未初始化")
                return result

            source_indexes = self._get_collection_indexes(self.source_mongo, database, collection)
            target_indexes = self._get_collection_indexes(self.target_mongo, database, collection)

            source_index_names = {idx['name'] for idx in source_indexes}
            target_index_names = {idx['name'] for idx in target_indexes}

            missing_indexes = source_index_names - target_index_names
            extra_indexes = target_index_names - source_index_names

            if missing_indexes:
                result['issues'].append(f"缺少索引: {', '.join(missing_indexes)}")
            if extra_indexes:
                result['issues'].append(f"多余索引: {', '.join(extra_indexes)}")

            # 添加数量差异信息
            if result['count_diff'] != 0:
                result['issues'].append(f"文档数量不一致: 源({result['source_count']}) vs 目标({result['target_count']})")

            # 数据哈希对比
            if detailed:
                if not self.source_mongo or not self.target_mongo:
                    result['issues'].append("MongoDB连接未初始化")
                    return result

                source_hash = self._calculate_collection_hash(self.source_mongo, database, collection)
                target_hash = self._calculate_collection_hash(self.target_mongo, database, collection)
                result['data_hash_match'] = source_hash == target_hash

                if not result['data_hash_match']:
                    result['issues'].append("数据内容不匹配")

        except Exception as e:
            result['issues'].append(f"对比过程中出错: {str(e)}")

        return result

    def compare_databases(self, databases: List[str] = None,
                         detailed: bool = False,
                         max_workers: int = 4) -> Dict[str, Any]:
        """对比指定数据库的数据"""
        if databases is None:
            databases = self.get_databases()
        elif not databases:
            databases = []

        # 使用实例默认值或参数值
        if detailed is None:
            detailed = self.detailed
        if max_workers is None:
            max_workers = self.max_workers

        results = {
            'summary': {
                'total_databases': len(databases) if databases else len(self.get_databases()),
                'checked_databases': 0,
                'total_collections': 0,
                'matching_collections': 0,
                'mismatched_collections': 0,
                'missing_collections': 0,
                'issues': []
            },
            'databases': {}
        }

        print(f"🔍 开始对比 {len(databases)} 个数据库...")

        for database in databases:
            print(f"\n📊 对比数据库: {database}")
            db_result = {
                'collections': {},
                'summary': {
                    'total_collections': 0,
                    'matching_collections': 0,
                    'mismatched_collections': 0,
                    'missing_collections': 0,
                    'issues': []
                }
            }

            try:
                # 获取源数据库的所有集合
                if not self.source_mongo or not self.target_mongo:
                    raise RuntimeError("MongoDB连接未初始化")

                source_collections = self.source_mongo.client[database].list_collection_names()
                target_collections = self.target_mongo.client[database].list_collection_names()

                # 过滤系统集合和被忽略的集合
                all_collections = set(source_collections + target_collections)
                all_collections = {c for c in all_collections if not c.startswith('system.')}

                # 过滤被忽略的集合
                ignore_patterns = []
                if self.global_config and self.global_config.ignore_collections:
                    ignore_patterns = [col.split('.')[1] for col in self.global_config.ignore_collections
                                     if col.startswith(f"{database}.")]
                collections_to_check = [c for c in all_collections if c not in ignore_patterns]

                db_result['summary']['total_collections'] = len(collections_to_check)
                results['summary']['total_collections'] += len(collections_to_check)

                print(f"   📂 需要对比的集合数量: {len(collections_to_check)}")

                # 使用线程池并发对比
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_collection = {
                        executor.submit(self._compare_collection_data, database, collection, detailed): collection
                        for collection in collections_to_check
                    }

                    for future in as_completed(future_to_collection):
                        collection = future_to_collection[future]
                        try:
                            collection_result = future.result()
                            db_result['collections'][collection] = collection_result

                            # 更新统计信息
                            if collection_result['issues']:
                                db_result['summary']['mismatched_collections'] += 1
                                results['summary']['mismatched_collections'] += 1
                            else:
                                db_result['summary']['matching_collections'] += 1
                                results['summary']['matching_collections'] += 1

                        except Exception as e:
                            db_result['collections'][collection] = {
                                'collection': collection,
                                'database': database,
                                'issues': [f"对比失败: {str(e)}"]
                            }
                            db_result['summary']['mismatched_collections'] += 1
                            results['summary']['mismatched_collections'] += 1

                results['summary']['checked_databases'] += 1
                results['databases'][database] = db_result

            except Exception as e:
                results['databases'][database] = {
                    'collections': {},
                    'summary': {
                        'total_collections': 0,
                        'matching_collections': 0,
                        'mismatched_collections': 0,
                        'missing_collections': 0,
                        'issues': [f"数据库对比失败: {str(e)}"]
                    }
                }
                results['summary']['issues'].append(f"数据库 {database} 对比失败: {str(e)}")

        return results

    def print_results(self, results: Dict[str, Any]) -> None:
        """打印对比结果到控制台"""
        print("=" * 80)
        print("MongoDB 数据对比报告")
        print("=" * 80)
        print(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("")

        # 汇总信息
        summary = results['summary']
        print("📊 汇总信息:")
        print(f"   总数据库数: {summary['total_databases']}")
        print(f"   已检查数据库: {summary['checked_databases']}")
        print(f"   总集合数: {summary['total_collections']}")
        print(f"   匹配集合: {summary['matching_collections']}")
        print(f"   不匹配集合: {summary['mismatched_collections']}")
        print("")

        # 详细对比结果
        for db_name, db_result in results['databases'].items():
            print(f"🗄️ 数据库: {db_name}")
            print("-" * 40)

            db_summary = db_result['summary']
            print(f"   总集合数: {db_summary['total_collections']}")
            print(f"   匹配集合: {db_summary['matching_collections']}")
            print(f"   不匹配集合: {db_summary['mismatched_collections']}")

            if db_summary['issues']:
                print("   ❌ 问题:")
                for issue in db_summary['issues']:
                    print(f"      - {issue}")

            # 显示不匹配的集合
            mismatched = []
            for collection, coll_result in db_result['collections'].items():
                if coll_result['issues']:
                    mismatched.append((collection, coll_result['issues']))

            if mismatched:
                print("   📝 不匹配的集合:")
                for collection, issues in mismatched:
                    print(f"      📂 {collection}:")
                    for issue in issues:
                        print(f"         ❌ {issue}")

            print("")

    def close(self):
        """关闭数据库连接"""
        if self.source_mongo:
            del self.source_mongo
        if self.target_mongo:
            del self.target_mongo


def main():
    """主函数"""
    try:
        # 创建数据对比器
        comparator = DataComparator()

        # 获取数据库列表
        databases = comparator.get_databases()

        print("🔍 MongoDB 数据对比工具")
        print(f"🗄️  目标数据库: {databases}")
        print("=" * 60)

        # 执行数据对比
        results = comparator.compare_databases()

        # 打印结果到控制台
        comparator.print_results(results)

        # 显示摘要
        summary = results['summary']
        print("\n" + "=" * 60)
        print("📊 对比结果摘要:")
        print(f"   总数据库数: {summary['total_databases']}")
        print(f"   已检查数据库: {summary['checked_databases']}")
        print(f"   总集合数: {summary['total_collections']}")
        print(f"   ✅ 匹配集合: {summary['matching_collections']}")
        print(f"   ❌ 不匹配集合: {summary['mismatched_collections']}")

        if summary['issues']:
            print(f"   ⚠️  问题: {len(summary['issues'])} 个")

        # 如果有不匹配，返回非零退出码
        if summary['mismatched_collections'] > 0:
            sys.exit(1)

    except Exception as e:
        print(f"❌ 执行失败: {e}")
        sys.exit(1)
    finally:
        try:
            if 'comparator' in locals():
                comparator.close()
        except NameError:
            pass
        except Exception:
            pass


if __name__ == "__main__":
    main()

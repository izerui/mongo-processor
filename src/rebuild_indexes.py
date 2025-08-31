#!/usr/bin/env python3
"""
重建索引测试脚本

该脚本用于读取配置文件并重建MongoDB数据库的索引。
支持从源数据库获取索引信息并在目标数据库上重建。

使用方法:
    python rebuild_indexes.py [--config CONFIG_PATH] [--databases DB1,DB2,...]

参数:
    --config: 配置文件路径 (默认: config.ini)
    --databases: 指定要重建索引的数据库，逗号分隔 (默认: 使用配置文件中的databases)
    --dry-run: 仅显示将要创建的索引，不实际执行
    --verbose: 显示详细日志
"""

import argparse
import sys
from configparser import ConfigParser
from pathlib import Path
from typing import List, Dict, Any

from base import MyMongo, MongoConfig, GlobalConfig


class IndexRebuilder:
    """索引重建器类"""

    def __init__(self, config_path: str = "../config.ini"):
        self.config_path = Path(__file__).parent.parent / config_path
        self.global_config = None
        self.source_mongo = None
        self.target_mongo = None
        self._load_config()

    def _load_config(self):
        """加载配置文件"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")

        config = ConfigParser()
        config.read(self.config_path)

        # 源数据库配置
        source_config = MongoConfig(
            config.get('source', 'host'),
            config.get('source', 'port'),
            config.get('source', 'username'),
            config.get('source', 'password')
        )

        # 目标数据库配置
        target_config = MongoConfig(
            config.get('target', 'host'),
            config.get('target', 'port'),
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

        # 初始化MongoDB连接
        self.source_mongo = MyMongo(source_config, self.global_config)
        self.target_mongo = MyMongo(target_config, self.global_config)

    def get_databases(self) -> List[str]:
        """获取需要处理的数据库列表"""
        return [db.strip() for db in self.global_config.databases if db.strip()]

    def analyze_indexes(self, databases: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        分析指定数据库的索引信息

        Args:
            databases: 数据库列表，如果为None则使用配置文件中的列表

        Returns:
            索引分析结果
        """
        if databases is None:
            databases = self.get_databases()

        analysis = {}

        for db_name in databases:
            print(f"\n🔍 分析数据库 {db_name} 的索引信息...")

            try:
                # 获取源数据库的索引信息
                source_indexes = self.source_mongo.get_database_indexes(db_name)

                # 获取目标数据库的索引信息
                target_indexes = self.target_mongo.get_database_indexes(db_name)

                # 分析差异
                analysis[db_name] = {
                    'source_indexes': source_indexes,
                    'target_indexes': target_indexes,
                    'missing_indexes': {},
                    'total_source_indexes': sum(len(idxs) for idxs in source_indexes.values()),
                    'total_target_indexes': sum(len(idxs) for idxs in target_indexes.values())
                }

                # 找出缺失的索引
                for collection, indexes in source_indexes.items():
                    missing = []
                    target_collection_indexes = {idx['name']: idx for idx in target_indexes.get(collection, [])}

                    for idx in indexes:
                        if idx['name'] not in target_collection_indexes:
                            missing.append(idx)

                    if missing:
                        analysis[db_name]['missing_indexes'][collection] = missing

                print(f"   📊 源数据库索引总数: {analysis[db_name]['total_source_indexes']}")
                print(f"   📊 目标数据库索引总数: {analysis[db_name]['total_target_indexes']}")
                print(f"   📊 缺失索引数量: {sum(len(idxs) for idxs in analysis[db_name]['missing_indexes'].values())}")

            except Exception as e:
                print(f"   ❌ 分析数据库 {db_name} 失败: {e}")
                analysis[db_name] = {'error': str(e)}

        return analysis

    def rebuild_indexes(self, databases: List[str] = None, dry_run: bool = False) -> Dict[str, Dict[str, Any]]:
        """
        重建指定数据库的索引

        Args:
            databases: 数据库列表，如果为None则使用配置文件中的列表
            dry_run: 是否仅模拟执行

        Returns:
            重建结果
        """
        if databases is None:
            databases = self.get_databases()

        results = {}

        # 分析索引
        analysis = self.analyze_indexes(databases)

        if dry_run:
            print("\n🎯 干运行模式 - 仅显示将要创建的索引:")
            for db_name, db_analysis in analysis.items():
                if 'error' in db_analysis:
                    continue

                missing = db_analysis['missing_indexes']
                if missing:
                    print(f"\n📂 数据库 {db_name} 需要创建的索引:")
                    for collection, indexes in missing.items():
                        print(f"   📝 集合 {collection}:")
                        for idx in indexes:
                            index_desc = f"{idx['name']}: {list(idx['key'].items())}"
                            if idx.get('unique'):
                                index_desc += " [唯一]"
                            if idx.get('sparse'):
                                index_desc += " [稀疏]"
                            print(f"      - {index_desc}")
                else:
                    print(f"   ✅ 数据库 {db_name} 索引已是最新")
            return analysis

        # 实际执行索引重建
        for db_name in databases:
            print(f"\n🔄 开始重建数据库 {db_name} 的索引...")

            try:
                db_analysis = analysis.get(db_name, {})
                if 'error' in db_analysis:
                    results[db_name] = {'error': db_analysis['error']}
                    continue

                # 获取源数据库的索引信息
                source_indexes = db_analysis.get('source_indexes', {})

                # 在目标数据库上创建索引
                creation_results = self.target_mongo.create_indexes_on_target(
                    db_name,
                    source_indexes
                )

                results[db_name] = {
                    'creation_results': creation_results,
                    'total_indexes_created': sum(1 for r in creation_results.values() if r),
                    'total_collections': len(creation_results)
                }

                print(f"   ✅ 成功创建 {results[db_name]['total_indexes_created']} 个索引")

            except Exception as e:
                print(f"   ❌ 重建数据库 {db_name} 索引失败: {e}")
                results[db_name] = {'error': str(e)}

        return results

    def close(self):
        """关闭数据库连接"""
        if self.source_mongo:
            del self.source_mongo
        if self.target_mongo:
            del self.target_mongo


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='重建MongoDB数据库索引')
    parser.add_argument('--config', default='config.ini', help='配置文件路径')
    parser.add_argument('--databases', help='指定数据库，逗号分隔')
    parser.add_argument('--dry-run', action='store_true', help='仅显示将要创建的索引')
    parser.add_argument('--verbose', action='store_true', help='显示详细日志')

    args = parser.parse_args()

    try:
        # 创建索引重建器
        rebuilder = IndexRebuilder(args.config)

        # 获取数据库列表
        databases = None
        if args.databases:
            databases = [db.strip() for db in args.databases.split(',') if db.strip()]

        print("🚀 MongoDB索引重建工具")
        print(f"📁 配置文件: {args.config}")
        print(f"🗄️  目标数据库: {databases or rebuilder.get_databases()}")
        print(f"🔍 运行模式: {'干运行' if args.dry_run else '实际执行'}")

        # 执行索引重建
        results = rebuilder.rebuild_indexes(databases, args.dry_run)

        # 显示总结
        print("\n" + "="*60)
        print("📊 重建结果总结:")

        total_databases = 0
        total_indexes = 0
        total_errors = 0

        for db_name, result in results.items():
            if 'error' in result:
                print(f"   ❌ {db_name}: {result['error']}")
                total_errors += 1
            else:
                if args.dry_run:
                    missing_count = sum(len(idxs) for idxs in result.get('missing_indexes', {}).values())
                    if missing_count > 0:
                        print(f"   📝 {db_name}: 需要创建 {missing_count} 个索引")
                        total_indexes += missing_count
                    else:
                        print(f"   ✅ {db_name}: 索引已是最新")
                else:
                    created = result.get('total_indexes_created', 0)
                    print(f"   ✅ {db_name}: 成功创建 {created} 个索引")
                    total_indexes += created
                total_databases += 1

        print(f"\n📈 总计: {total_databases} 个数据库, {total_indexes} 个索引")
        if total_errors > 0:
            print(f"⚠️  错误: {total_errors} 个数据库处理失败")

    except Exception as e:
        print(f"❌ 执行失败: {e}")
        sys.exit(1)
    finally:
        if 'rebuilder' in locals():
            rebuilder.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from configparser import ConfigParser
from pathlib import Path

from dump import MongoConfig
from src.base import GlobalConfig
from src.manager import Manager


def main():
    """主函数 - 使用线程池并发处理"""
    config = ConfigParser()
    config_path = Path(__file__).parent.parent / 'config.ini'
    config.read(config_path)

    source_config = MongoConfig(
        config.get('source', 'host'),
        config.get('source', 'port'),
        config.get('source', 'username'),
        config.get('source', 'password')
    )

    target_config = MongoConfig(
        config.get('target', 'host'),
        config.get('target', 'port'),
        config.get('target', 'username'),
        config.get('target', 'password')
    )

    # 全局配置
    global_config = GlobalConfig()
    global_config.databases = config.get('global', 'databases').split(',')
    global_config.skip_export = config.getboolean('global', 'skipExport', fallback=False)
    global_config.numParallelCollections = config.getint('global', 'numParallelCollections')
    global_config.maxThreads = config.getint('global', 'maxThreads', fallback=4)
    global_config.numInsertionWorkersPerCollection = config.getint('global', 'numInsertionWorkersPerCollection')

    # 分片配置
    global_config.enable_sharding = config.getboolean('global', 'enableSharding', fallback=True)
    global_config.min_documents_for_shard = config.getint('global', 'minDocumentsForShard', fallback=1000000)
    global_config.default_shard_count = config.getint('global', 'defaultShardCount', fallback=4)
    global_config.max_shard_count = config.getint('global', 'maxShardCount', fallback=16)

    # 读取忽略的集合配置
    ignore_collections_str = config.get('global', 'ignoreCollections', fallback='')
    global_config.ignore_collections = [col.strip() for col in ignore_collections_str.split(',') if col.strip()]

    global_config.dump_root_path = Path(__file__).parent.parent / 'dumps'

    print(f"⚙️ 导出配置: 单库并发数={global_config.numParallelCollections}, 线程池并发数={global_config.maxThreads}, 跳过导出={global_config.skip_export}")
    print(f"🔄 分片配置: 启用分片={global_config.enable_sharding}, 分片阈值={global_config.min_documents_for_shard:,}条, 最大分片数={global_config.max_shard_count}")
    print(f"🚫 忽略集合: {len(global_config.ignore_collections)}个")
    if global_config.ignore_collections:
        for col in global_config.ignore_collections:
            print(f"    - {col}")
    print(f"📊 待处理数据库: {len(global_config.databases)}个")

    manager = Manager(source_config=source_config, target_config=target_config, global_config=global_config)
    manager.dump_and_restore()

if __name__ == "__main__":
    main()

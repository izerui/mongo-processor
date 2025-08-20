#!/usr/bin/env python3
import argparse
import concurrent.futures
import os
import shutil
import sys
import time
from configparser import ConfigParser
from pathlib import Path
from typing import Tuple, Optional

from dump import MyDump, Mongo
from restore import MyRestore
from base import ShardConfig
from index import IndexManager, create_mongo_client


def cleanup_dump_folder(dump_folder: Path) -> None:
    """清理历史导出目录"""
    if dump_folder.exists():
        # 只删除目录内容，不删除目录本身（云盘挂载路径）
        for item in dump_folder.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)


def process_single_database(db_name: str, source: Mongo, target: Mongo,
                            numParallelCollections: int, numInsertionWorkersPerCollection: int, dump_folder: Path,
                            enable_sharding: bool = True, shard_config: Optional[ShardConfig] = None,
                            skip_export: bool = False) -> \
Tuple[str, bool, float, float, float]:
    """
    处理单个数据库的导出、导入和清理
    :param db_name: 数据库名称
    :param source: 源MongoDB连接信息
    :param target: 目标MongoDB连接信息
    :param numParallelCollections: 并发数
    :param numInsertionWorkersPerCollection: 每个集合的插入工作线程数
    :param dump_folder: 导出目录
    :param enable_sharding: 是否启用分片
    :param shard_config: 分片配置
    :param skip_export: 是否跳过导出步骤
    :return: (数据库名, 是否成功, 总耗时, 导出时间, 导入时间)
    """
    start_time = time.time()
    export_time = 0.0
    import_time = 0.0

    try:
        # 导出
        export_start_time = time.time()
        if not skip_export:
            mydump = MyDump(source, numParallelCollections, enable_sharding, shard_config)
            mydump.export_db(database=db_name, dump_root_path=str(dump_folder))
            export_time = time.time() - export_start_time
            print(f' ✅ 成功从{source.host}导出: {db_name} (耗时: {export_time:.2f}秒)')
        else:
            export_time = 0.0
            print(f' ⏭️  跳过导出: {db_name} (使用已有导出数据)')

        # 导入
        import_start_time = time.time()
        myrestore = MyRestore(target, numParallelCollections, numInsertionWorkersPerCollection)
        myrestore.restore_db(database=db_name, dump_root_path=str(dump_folder))
        import_time = time.time() - import_start_time
        print(f' ✅ 成功导入{target.host}: {db_name} (耗时: {import_time:.2f}秒)')

        total_time = time.time() - start_time
        return db_name, True, total_time, export_time, import_time

    except Exception as e:
        total_time = time.time() - start_time
        print(f' ❌ 处理数据库 {db_name} 失败: {str(e)}')
        return db_name, False, total_time, export_time, import_time


def main():
    """主函数 - 使用线程池并发处理"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='MongoDB数据迁移工具')
    parser.add_argument('--skip-export', action='store_true',
                       help='跳过导出步骤，直接使用已有导出数据进行导入')
    parser.add_argument('--config', type=str, default='config.ini',
                       help='配置文件路径 (默认: config.ini)')

    args = parser.parse_args()

    config = ConfigParser()
    config_path = Path(__file__).parent.parent / args.config
    config.read(config_path)

    source = Mongo(
        config.get('source', 'host'),
        config.get('source', 'port'),
        config.get('source', 'username'),
        config.get('source', 'password')
    )

    target = Mongo(
        config.get('target', 'host'),
        config.get('target', 'port'),
        config.get('target', 'username'),
        config.get('target', 'password')
    )

    databases = config.get('global', 'databases').split(',')
    maxThreads = config.getint('global', 'maxThreads', fallback=4)  # 新增配置项
    numParallelCollections = config.getint('global', 'numParallelCollections')
    numInsertionWorkersPerCollection = config.getint('global', 'numInsertionWorkersPerCollection')
    # 命令行参数优先级高于配置文件
    skip_export = args.skip_export or config.getboolean('global', 'skipExport', fallback=False)

    # 分片相关配置
    enable_sharding = config.getboolean('global', 'enableSharding', fallback=True)
    min_documents_for_shard = config.getint('global', 'minDocumentsForShard', fallback=1000000)
    default_shard_count = config.getint('global', 'defaultShardCount', fallback=4)
    max_shard_count = config.getint('global', 'maxShardCount', fallback=16)

    # 创建分片配置
    shard_config = ShardConfig()
    shard_config.min_documents_for_shard = min_documents_for_shard
    shard_config.default_shard_count = default_shard_count
    shard_config.max_shard_count = max_shard_count

    dump_folder = Path(__file__).parent.parent / 'dumps'

    # 清理历史导出目录（仅在需要导出时）
    if not skip_export:
        cleanup_dump_folder(dump_folder)
        dump_folder.mkdir(exist_ok=True)
    else:
        print("⚠️  跳过导出模式，保留现有导出数据")

    print(f"⚙️ 导出配置: 单库并发数={numParallelCollections}, 线程池并发数={maxThreads}, 跳过导出={skip_export}")
    print(f"🔄 分片配置: 启用分片={enable_sharding}, 分片阈值={min_documents_for_shard:,}条, 最大分片数={max_shard_count}")
    print(f"📊 待处理数据库: {len(databases)}个")

    total_start_time = time.time()

    # 使用线程池并发处理每个数据库
    successful_dbs = []
    failed_dbs = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=maxThreads) as pool:
        # 提交所有数据库处理任务
        future_to_db = {
            pool.submit(process_single_database, db.strip(), source, target,
                        numParallelCollections, numInsertionWorkersPerCollection, dump_folder,
                        enable_sharding, shard_config, skip_export): db.strip()
            for db in databases
        }

        # 处理完成的任务
        try:
            for future in concurrent.futures.as_completed(future_to_db):
                db_name = future_to_db[future]
                try:
                    db_name, success, duration, export_time, import_time = future.result()
                    if success:
                        successful_dbs.append((db_name, duration, export_time, import_time))
                        print(f' 🎉 数据库 {db_name} 处理完成 (耗时: {duration:.2f}秒)')
                    else:
                        failed_dbs.append((db_name, duration, export_time, import_time))
                        print(f' 💥 数据库 {db_name} 处理失败 (耗时: {duration:.2f}秒)')

                except KeyboardInterrupt:
                    print(f" ⚠️  用户中断处理数据库: {db_name}")
                    failed_dbs.append((db_name, 0, 0, 0))
                    break
                except TimeoutError as e:
                    print(f" ⏰ 数据库 {db_name} 处理超时: {str(e)}")
                    failed_dbs.append((db_name, 0, 0, 0))
                except Exception as e:
                    failed_dbs.append((db_name, 0, 0, 0))
                    print(f' 💥 数据库 {db_name} 处理时发生异常: {str(e)}')
        except KeyboardInterrupt:
            print("\n ⚠️  收到中断信号，正在优雅退出...")
            # 取消所有未完成的任务
            for future in future_to_db:
                future.cancel()
            print(" ✅ 已取消所有未完成的任务")

    total_time = time.time() - total_start_time

    # 计算总计时间
    total_export_time = sum(db[2] for db in successful_dbs) + sum(db[2] for db in failed_dbs)
    total_import_time = sum(db[3] for db in successful_dbs) + sum(db[3] for db in failed_dbs)

    # 打印统计信息
    print(f"\n📈 处理完成统计:")
    print(f"   ✅ 成功: {len(successful_dbs)}个数据库")
    for db_name, duration, export_time, import_time in successful_dbs:
        print(f"      - {db_name}: {duration:.2f}秒 (导出: {export_time:.2f}秒, 导入: {import_time:.2f}秒)")

    if failed_dbs:
        print(f"   ❌ 失败: {len(failed_dbs)}个数据库")
        for db_name, duration, export_time, import_time in failed_dbs:
            print(f"      - {db_name}: {duration:.2f}秒 (导出: {export_time:.2f}秒, 导入: {import_time:.2f}秒)")

    print(f"\n📊 总计统计:")
    print(f"   总导出时间: {total_export_time:.2f}秒")
    print(f"   总导入时间: {total_import_time:.2f}秒")
    print(f"   总处理时间: {total_time:.2f}秒")
    if total_export_time + total_import_time > 0:
        print(f"   并行效率: {total_time / (total_export_time + total_import_time):.2f}x")
    else:
        print(f"   并行效率: N/A")

    print(f' 🎯 所有数据库操作完成，总耗时: {total_time:.2f}秒')

    # 如果有失败的数据库，打印错误信息
    if failed_dbs:
        print("⚠️  部分数据库处理失败，请检查日志")
        print("   失败的数据库:")
        for db_name, duration, export_time, import_time in failed_dbs:
            print(f"      - {db_name}")

    # 程序结束
    # 为成功的数据库创建索引
    if successful_dbs:
        print("\n🔍 开始创建索引...")
        print("=" * 50)

        try:
            # 创建MongoDB客户端
            source_client = create_mongo_client(
                source.host,
                int(source.port),
                source.username or "",
                source.password or ""
            )

            target_client = create_mongo_client(
                target.host,
                int(target.port),
                target.username or "",
                target.password or ""
            )

            # 创建索引管理器
            index_manager = IndexManager(source_client, target_client)

            # 获取成功的数据库名列表
            successful_db_names = [db[0] for db in successful_dbs]

            # 并发创建索引
            index_results = index_manager.recreate_indexes_for_databases(
                successful_db_names,
                max_workers=maxThreads
            )

            # 打印索引创建统计
            print("\n" + "=" * 50)
            print(f"📊 索引创建汇总统计:")
            print(f"   📂 处理数据库数: {index_results['total_databases']}")
            print(f"   ✅ 成功数据库数: {index_results['successful_databases']}")
            print(f"   📈 总索引数: {index_results['total_indexes']}")
            print(f"   🎯 成功创建索引数: {index_results['created_indexes']}")
            print(f"   ⏱️  总耗时: {index_results['duration']:.2f}秒")

            if index_results['failed_databases']:
                print(f"\n   ❌ 失败数据库:")
                for failed_db in index_results['failed_databases']:
                    print(f"      📛 {failed_db['database']}: {failed_db.get('error', '未知错误')}")
            else:
                print(f"\n   🎉 所有数据库索引创建成功!")

            # 关闭客户端连接
            source_client.close()
            target_client.close()

        except Exception as e:
            print(f"⚠️  索引创建过程中发生错误: {e}")

    print("💤 程序执行完成，进入休眠状态...")

    try:
        while True:
            time.sleep(3600)  # 每小时检查一次
    except KeyboardInterrupt:
        print("收到退出信号，程序结束")
        sys.exit(0)


if __name__ == "__main__":
    main()

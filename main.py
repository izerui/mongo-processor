#!/usr/bin/env python3
import os
import platform
import shutil
import sys
import time
import concurrent.futures
import subprocess
from configparser import ConfigParser
from pathlib import Path

from typing import List, Tuple
from dump import MyDump, MyImport, Mongo


def cleanup_dump_folder(dump_folder: Path) -> None:
    """清理历史导出目录"""
    if dump_folder.exists():
        # 只删除目录内容，不删除目录本身（云盘挂载路径）
        for item in dump_folder.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)


def get_large_collections_info(source: Mongo, database: str,
                             threshold_docs: int = 1000000) -> List[Tuple[str, int]]:
    """
    获取数据库中的大集合信息

    :param source: MongoDB连接信息
    :param database: 数据库名
    :param threshold_docs: 大集合阈值
    :return: [(集合名, 文档数量), ...]
    """
    auth_append = f'--username={source.username} --password="{source.password}" --authenticationDatabase=admin' if source.username else ''

    try:
        # 获取所有集合
        collections_cmd = f'mongo --host="{source.host}:{source.port}" {auth_append} --quiet --eval "db.getSiblingDB(\'{database}\').getCollectionNames()" --norc'
        result = subprocess.run(collections_cmd, shell=True, capture_output=True, text=True)
        collections = eval(result.stdout.strip())

        large_collections = []
        for collection in collections:
            # 获取文档数量
            count_cmd = f'mongo --host="{source.host}:{source.port}" {auth_append} --quiet --eval "db.getSiblingDB(\'{database}\').{collection}.countDocuments()" --norc'
            count_result = subprocess.run(count_cmd, shell=True, capture_output=True, text=True)
            try:
                doc_count = int(count_result.stdout.strip())
                if doc_count >= threshold_docs:
                    large_collections.append((collection, doc_count))
            except:
                continue

        return large_collections
    except Exception as e:
        print(f"⚠️  获取大集合信息失败: {str(e)}")
        return []


def process_single_database(db_name: str, source: Mongo, target: Mongo,
                            numParallelCollections: int, numInsertionWorkersPerCollection: int, dump_folder: Path,
                            large_collection_threshold: int = 1000000) -> Tuple[str, bool, float]:
    """
    处理单个数据库的导出、导入和清理
    :param db_name: 数据库名称
    :param source: 源MongoDB连接信息
    :param target: 目标MongoDB连接信息
    :param numParallelCollections: 并发数
    :param numInsertionWorkersPerCollection: 每个集合的插入工作线程数
    :param dump_folder: 导出目录
    :param large_collection_threshold: 大集合阈值，超过此值使用分区导出
    :return: (数据库名, 是否成功, 总耗时)
    """
    start_time = time.time()
    try:
        # 获取大集合信息
        large_collections = get_large_collections_info(source, db_name, large_collection_threshold)

        export_start_time = time.time()

        if large_collections:
            print(f' ℹ️ 从{source.host}导出: {db_name} (检测到 {len(large_collections)} 个大集合)')

            # 处理大集合的分区导出
            mydump = MyDump(source, numParallelCollections)
            all_partition_dirs = []

            for collection_name, doc_count in large_collections:
                print(f'   📊 大集合 {collection_name}: {doc_count:,} 条文档，使用分区导出')

                # 计算分区数量：每50万条一个分区，最多8个
                partitions = min(8, max(2, doc_count // 500000))

                # 分区导出大集合
                partition_dirs = mydump.export_collection_partitioned(
                    database=db_name,
                    collection=collection_name,
                    dump_root_path=str(dump_folder),
                    partition_field="_id",
                    partitions=partitions
                )
                all_partition_dirs.extend(partition_dirs)

            # 普通导出剩余的小集合
            print(f'   📊 导出剩余小集合...')
            auth_append = f'--username={source.username} --password="{source.password}" --authenticationDatabase=admin' if source.username else ''

            # 根据平台选择正确的mongodump路径
            if platform.system() == 'Windows':
                mongodump_exe = os.path.join('mongodb-database-tools', 'windows-x86_64-100.13.0', 'mongodump.exe')
            elif platform.system() == 'Linux':
                mongodump_exe = os.path.join('mongodb-database-tools', 'rhel93-x86_64-100.13.0', 'mongodump')
            elif platform.system() == 'Darwin':
                mongodump_exe = os.path.join('mongodb-database-tools', 'macos-arm64-100.13.0', 'mongodump')

            export_cmd = (
                f'{mongodump_exe} '
                f'--host="{source.host}:{source.port}" '
                f'--db={db_name} '
                f'--out={dump_folder} '
                f'--numParallelCollections={numParallelCollections} '
                f'--gzip {auth_append} '
            )

            # 排除已处理的大集合
            exclude_collections = ' '.join([f'--excludeCollection={collection_name}' for collection_name, _ in large_collections])
            export_cmd += exclude_collections

            mydump._exe_command(export_cmd)

        else:
            # 没有大集合，使用标准导出
            print(f' ℹ️ 从{source.host}导出: {db_name} (无大集合，使用标准导出)')
            mydump = MyDump(source, numParallelCollections)
            mydump.export_db(db_name, dump_folder)

        export_time = time.time() - export_start_time
        print(f' ✅ 成功从{source.host}导出: {db_name} (耗时: {export_time:.2f}秒)')

        db_dir = os.path.join(dump_folder, db_name)

        # 导入uat
        print(f' ℹ️ 导入{target.host}: {db_name}')
        import_start_time = time.time()

        if large_collections:
            # 处理分区导出的导入
            myimport = MyImport(target, numParallelCollections, numInsertionWorkersPerCollection)

            # 导入普通集合
            if os.path.exists(db_dir):
                myimport.import_db(db_name, db_dir)

            # 并发导入大集合的分区
            for collection_name, _ in large_collections:
                partition_dirs = []
                for i in range(8):  # 最多检查8个分区
                    partition_dir = os.path.join(dump_folder, f"{db_name}_{collection_name}_part{i}")
                    if os.path.exists(partition_dir):
                        partition_dirs.append(partition_dir)

                if partition_dirs:
                    print(f'   🔄 并发导入大集合 {collection_name} 的 {len(partition_dirs)} 个分区...')
                    myimport.import_partitioned_collection(db_name, partition_dirs)

        else:
            # 标准导入
            myimport = MyImport(target, numParallelCollections, numInsertionWorkersPerCollection)
            myimport.import_db(db_name, db_dir)

        import_time = time.time() - import_start_time
        print(f' ✅ 成功导入{target.host}: {db_name} (耗时: {import_time:.2f}秒)')

        # 删除导出的文件
        print(f' ✅ 删除临时文件缓存: {db_dir}')
        if os.path.exists(db_dir):
            shutil.rmtree(db_dir)

        total_time = time.time() - start_time
        return db_name, True, total_time

    except Exception as e:
        total_time = time.time() - start_time
        print(f' ❌ 处理数据库 {db_name} 失败: {str(e)}')
        return db_name, False, total_time


def main():
    """主函数 - 使用线程池并发处理"""
    config = ConfigParser()
    config.read('config.ini')

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
    large_collection_threshold = config.getint('global', 'largeCollectionThreshold', fallback=1000000)  # 大集合阈值

    dump_folder = Path(__file__).parent / 'dumps'

    # 清理历史导出目录
    cleanup_dump_folder(dump_folder)
    dump_folder.mkdir(exist_ok=True)

    print(f"⚙️ 导出配置: 单库并发数={numParallelCollections}, 线程池并发数={maxThreads}, 大集合阈值={large_collection_threshold:,}")
    print(f"📊 待处理数据库: {len(databases)}个")

    total_start_time = time.time()

    # 使用线程池并发处理每个数据库
    successful_dbs = []
    failed_dbs = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=maxThreads) as pool:
        # 提交所有数据库处理任务
        future_to_db = {
            pool.submit(process_single_database, db.strip(), source, target,
                        numParallelCollections, numInsertionWorkersPerCollection, dump_folder, large_collection_threshold): db.strip()
            for db in databases
        }

        # 处理完成的任务
        try:
            for future in concurrent.futures.as_completed(future_to_db):
                db_name = future_to_db[future]
                try:
                    db_name, success, duration = future.result()
                    if success:
                        successful_dbs.append((db_name, duration))
                        print(f' 🎉 数据库 {db_name} 处理完成 (耗时: {duration:.2f}秒)')
                    else:
                        failed_dbs.append((db_name, duration))
                        print(f' 💥 数据库 {db_name} 处理失败 (耗时: {duration:.2f}秒)')

                except KeyboardInterrupt:
                    print(f" ⚠️  用户中断处理数据库: {db_name}")
                    failed_dbs.append((db_name, 0))
                    break
                except TimeoutError as e:
                    print(f" ⏰ 数据库 {db_name} 处理超时: {str(e)}")
                    failed_dbs.append((db_name, 0))
                except Exception as e:
                    failed_dbs.append((db_name, 0))
                    print(f' 💥 数据库 {db_name} 处理时发生异常: {str(e)}')
        except KeyboardInterrupt:
            print("\n ⚠️  收到中断信号，正在优雅退出...")
            # 取消所有未完成的任务
            for future in future_to_db:
                future.cancel()
            print(" ✅ 已取消所有未完成的任务")

    total_time = time.time() - total_start_time

    # 打印统计信息
    print(f"\n📈 处理完成统计:")
    print(f"   ✅ 成功: {len(successful_dbs)}个数据库")
    for db_name, duration in successful_dbs:
        print(f"      - {db_name}: {duration:.2f}秒")

    if failed_dbs:
        print(f"   ❌ 失败: {len(failed_dbs)}个数据库")
        for db_name, duration in failed_dbs:
            print(f"      - {db_name}: {duration:.2f}秒")

    print(f' 🎯 所有数据库操作完成，总耗时: {total_time:.2f}秒')

    # 如果有失败的数据库，退出码为非0
    if failed_dbs:
        print("⚠️  部分数据库处理失败，请检查日志")
        # 不强制退出，让用户选择是否重试
        retry = input("是否重试失败的数据库？(y/n): ").lower().strip()
        if retry == 'y':
            # 重新处理失败的数据库
            failed_db_names = [db[0] for db in failed_dbs]
            print(f"重新处理失败的数据库: {failed_db_names}")
            # 这里可以添加重试逻辑

    # 程序结束
    print("💤 程序执行完成，进入休眠状态...")

    try:
        while True:
            time.sleep(3600)  # 每小时检查一次
    except KeyboardInterrupt:
        print("收到退出信号，程序结束")
        sys.exit(0)


if __name__ == "__main__":
    main()

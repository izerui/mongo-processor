#!/usr/bin/env python3
import concurrent.futures
import os
import shutil
import sys
import time
from configparser import ConfigParser
from pathlib import Path
from typing import Tuple

from dump import MyDump, Mongo
from restore import MyRestore


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
                            numParallelCollections: int, numInsertionWorkersPerCollection: int, dump_folder: Path) -> \
Tuple[str, bool, float]:
    """
    处理单个数据库的导出、导入和清理
    :param db_name: 数据库名称
    :param source: 源MongoDB连接信息
    :param target: 目标MongoDB连接信息
    :param numParallelCollections: 并发数
    :param numInsertionWorkersPerCollection: 每个集合的插入工作线程数
    :param dump_folder: 导出目录
    :return: (数据库名, 是否成功, 总耗时)
    """
    start_time = time.time()
    try:
        # 导出
        export_start_time = time.time()
        mydump = MyDump(source, numParallelCollections)
        db_dump_dir = mydump.export_db(database=db_name, dump_root_path=str(dump_folder))
        export_time = time.time() - export_start_time
        print(f' ✅ 成功从{source.host}导出: {db_name} (耗时: {export_time:.2f}秒)')

        # 导入
        import_start_time = time.time()
        myrestore = MyRestore(target, numParallelCollections, numInsertionWorkersPerCollection)
        myrestore.restore_db(database=db_name, dump_root_path=str(dump_folder))
        import_time = time.time() - import_start_time
        print(f' ✅ 成功导入{target.host}: {db_name} (耗时: {import_time:.2f}秒)')

        # 删除当前数据库的导出目录
        db_export_dir = os.path.join(str(dump_folder), db_name)
        print(f' ✅ 删除临时文件缓存: {db_export_dir}')
        if os.path.exists(db_export_dir):
            shutil.rmtree(db_export_dir)

        total_time = time.time() - start_time
        return db_name, True, total_time

    except Exception as e:
        total_time = time.time() - start_time
        print(f' ❌ 处理数据库 {db_name} 失败: {str(e)}')
        return db_name, False, total_time


def main():
    """主函数 - 使用线程池并发处理"""
    config = ConfigParser()
    config_path = Path(__file__).parent.parent / 'config.ini'
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

    dump_folder = Path(__file__).parent.parent / 'dumps'

    # 清理历史导出目录
    cleanup_dump_folder(dump_folder)
    dump_folder.mkdir(exist_ok=True)

    print(f"⚙️ 导出配置: 单库并发数={numParallelCollections}, 线程池并发数={maxThreads}")
    print(f"📊 待处理数据库: {len(databases)}个")

    total_start_time = time.time()

    # 使用线程池并发处理每个数据库
    successful_dbs = []
    failed_dbs = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=maxThreads) as pool:
        # 提交所有数据库处理任务
        future_to_db = {
            pool.submit(process_single_database, db.strip(), source, target,
                        numParallelCollections, numInsertionWorkersPerCollection, dump_folder): db.strip()
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

    # 如果有失败的数据库，打印错误信息
    if failed_dbs:
        print("⚠️  部分数据库处理失败，请检查日志")
        print("   失败的数据库:")
        for db_name, duration in failed_dbs:
            print(f"      - {db_name}")

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

#!/usr/bin/env python3
"""
索引处理模块
用于从source MongoDB读取索引信息，并在target MongoDB后台创建索引
"""
import concurrent.futures
import logging
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Tuple

from src.base import GlobalConfig, MongoConfig
from src.dump import MyDump
from src.restore import MyRestore

logger = logging.getLogger(__name__)


class Manager:
    """索引管理器，负责读取和创建索引"""

    def __init__(self, source_config: MongoConfig, target_config: MongoConfig, global_config: GlobalConfig):
        """
        初始化索引管理器

        Args:
            source_config: 源MongoDB客户端
            target_config: 目标MongoDB客户端
        """
        self.source_config = source_config
        self.target_config = target_config
        self.global_config = global_config
        self.source_mongo = MyDump(source_config, global_config)
        self.target_mongo = MyRestore(target_config, global_config)

    def process_single_database(self, db_name: str) -> Tuple[str, bool, float, float, float]:
        """
        处理单个数据库的导出、导入和清理
        :param db_name: 数据库名称
        :return: (数据库名, 是否成功, 总耗时, 导出时间, 导入时间)
        """
        start_time = time.time()
        export_time = 0.0
        import_time = 0.0

        try:
            # 导出
            export_start_time = time.time()
            if not self.global_config.skip_export:
                self.source_mongo.export_db(database=db_name)
                export_time = time.time() - export_start_time
                print(f' ✅ 成功从{self.source_config.host}导出: {db_name} (耗时: {export_time:.2f}秒)')
            else:
                export_time = 0.0
                print(f' ⏭️  跳过导出: {db_name} (使用已有导出数据)')

            # 导入
            import_start_time = time.time()
            self.target_mongo.restore_db(database=db_name)
            import_time = time.time() - import_start_time
            print(f' ✅ 成功导入{self.target_config.host}: {db_name} (耗时: {import_time:.2f}秒)')

            total_time = time.time() - start_time
            return db_name, True, total_time, export_time, import_time

        except Exception as e:
            total_time = time.time() - start_time
            print(f' ❌ 处理数据库 {db_name} 失败: {str(e)}')
            return db_name, False, total_time, export_time, import_time

    def cleanup_dump_folder(self) -> None:
        """清理历史导出目录"""
        if self.global_config.dump_root_path.exists():
            # 只删除目录内容，不删除目录本身（云盘挂载路径）
            for item in self.global_config.dump_root_path.iterdir():
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)

    def dump_and_restore(self):

        # 清理历史导出目录（仅在需要导出时）
        if not self.global_config.skip_export:
            self.cleanup_dump_folder()
            self.global_config.dump_root_path.mkdir(exist_ok=True)
        else:
            print("⚠️  跳过导出模式，保留现有导出数据")

        total_start_time = time.time()

        # 使用线程池并发处理每个数据库
        successful_dbs = []
        failed_dbs = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.global_config.maxThreads) as pool:
            # 提交所有数据库处理任务
            future_to_db = {
                pool.submit(self.process_single_database, db.strip()): db.strip()
                for db in self.global_config.databases
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
                # 获取成功的数据库名列表
                successful_db_names = [db[0] for db in successful_dbs]

                # 并发创建索引
                index_results = self.recreate_indexes_for_databases(
                    successful_db_names
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

            except Exception as e:
                print(f"⚠️  索引创建过程中发生错误: {e}")

        print("✅ 数据导出导入程序执行完成")

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

    def recreate_indexes_for_databases(self, database_names: List[str]) -> Dict[str, Any]:
        """
        并发重新创建多个数据库的索引

        Args:
            database_names: 数据库名称列表

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
        with ThreadPoolExecutor(max_workers=self.global_config.maxThreads) as executor:
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

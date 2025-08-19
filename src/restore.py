import os
import platform
from subprocess import Popen, PIPE, STDOUT
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient

from base import Shell, Mongo, mongorestore_exe


class MyRestore(Shell):
    """
    按数据库整体导入
    """

    def __init__(self, mongo: Mongo, num_parallel_collections: int = 4, num_insertion_workers: int = 4):
        super().__init__()
        self.mongo = mongo
        self.num_parallel_collections = num_parallel_collections
        self.num_insertion_workers = num_insertion_workers

    def restore_db(self, database: str, dump_root_path: str) -> None:
        """
        按数据库整体导入
        :param database: 目标数据库名
        :param dump_dirs: 导出的数据库目录路径列表
        """
        if not dump_root_path:
            print(f"⚠️ 没有提供数据库目录路径")
            return

        db_dir = os.path.join(dump_root_path, database)

        if not os.path.exists(db_dir):
            print(f"⚠️ 数据库目录不存在: {db_dir}")
            return

        try:
            # 构建认证参数
            user_append = f'--username="{self.mongo.username}"' if self.mongo.username else ''
            password_append = f'--password="{self.mongo.password}"' if self.mongo.password else ''
            auth_append = f'--authenticationDatabase=admin' if self.mongo.username else ''

            # 构建导入命令 - 直接导入整个数据库
            import_cmd = (
                f'{mongorestore_exe} '
                f'--host="{self.mongo.host}:{self.mongo.port}" '
                f'{user_append} {password_append} {auth_append} '
                f'--numParallelCollections={self.num_parallel_collections} '
                f'--numInsertionWorkersPerCollection={self.num_insertion_workers} '
                f'--noIndexRestore '
                f'--drop '
                f'--gzip '
                f'--dir="{dump_root_path}"'
            )

            # 执行导入
            self._exe_command(import_cmd)
            print(f'✅ 数据库 {database} 导入完成')

        except Exception as e:
            print(f'❌ 导入数据库 {database} 失败: {e}')
            raise

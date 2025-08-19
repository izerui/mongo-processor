import os
import platform
from subprocess import Popen, PIPE, STDOUT
from typing import List
from pymongo import MongoClient

from base import Shell, Mongo, mongodump_exe


class MyDump(Shell):
    """
    按数据库整体导出
    """

    def __init__(self, mongo: Mongo, numParallelCollections: int = 4):
        super().__init__()
        self.mongo = mongo
        self.numParallelCollections = numParallelCollections

    def export_db(self, database: str, dump_root_path: str) -> List[str]:
        """
        按数据库整体导出
        :param database: 数据库名
        :param dump_root_path: 导出根目录
        :return: 导出的数据库目录路径列表
        """
        try:
            # 构建导出目录：dumps/（mongodump会自动创建数据库名子目录）
            output_dir = dump_root_path

            # 构建认证参数
            auth_append = ''
            if self.mongo.username and self.mongo.password:
                auth_append = f'--username={self.mongo.username} --password="{self.mongo.password}" --authenticationDatabase=admin'

            # 构建导出命令 - 直接导出整个数据库
            export_cmd = (
                f'{mongodump_exe} '
                f'--host="{self.mongo.host}:{self.mongo.port}" '
                f'--db={database} '
                f'--out={output_dir} '
                f'--numParallelCollections={self.numParallelCollections} '
                f'{auth_append}'
            )

            # 执行导出
            self._exe_command(export_cmd)

            # 返回数据库目录路径
            return  os.path.join(dump_root_path, database)

        except Exception as e:
            print(f'❌ 导出数据库 {database} 失败: {e}')
            return []

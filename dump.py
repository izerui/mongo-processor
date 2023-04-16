import os
import platform
from subprocess import Popen, PIPE, STDOUT

if platform.system() == 'Windows':
    mongodump_exe = os.path.join('mongo-client', '4.0', 'windows', 'mongodump.exe')
    mongorestore_exe = os.path.join('mongo-client', '4.0', 'windows', 'mongorestore.exe')
elif platform.system() == 'Linux':
    raise BaseException('暂不支持')
elif platform.system() == 'Darwin':
    mongodump_exe = os.path.join('mongo-client', '4.0', 'osx', 'mongodump')
    mongorestore_exe = os.path.join('mongo-client', '4.0', 'osx', 'mongorestore')


class Shell(object):

    def _exe_command(self, command):
        """
        执行 shell 命令并实时打印输出
        :param command: shell 命令
        :return: process, exitcode
        """
        print(command)
        process = Popen(command, stdout=PIPE, stderr=STDOUT, shell=True)
        with process.stdout:
            for line in iter(process.stdout.readline, b''):
                print(line.decode().strip())
        exitcode = process.wait()
        if exitcode != 0:
            raise BaseException('命令执行失败')
        return process, exitcode


class Mongo:
    __slots__ = ['db_host', 'db_port', 'db_user', 'db_pass']

    def __init__(self, db_host, db_port, db_user, db_pass):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pass = db_pass


class MyDump(Shell):
    """
    导出数据库备份到目录
    """

    def __init__(self, mongo: Mongo):
        super().__init__()
        self.mongo = mongo

    def export_db(self, database, dump_root_path):
        """
        导出mongo数据库到dump目录
        :param database: 数据库名
        :param dump_root_path: dump根目录,会自动创建对应数据库名的dump目录
        :return:
        """
        export_shell = f'''{mongodump_exe} --host="{self.mongo.db_host}:{self.mongo.db_port}" --db={database} --out={dump_root_path} --numParallelCollections 4 --username={self.mongo.db_user} --password="{self.mongo.db_pass}"'''
        self._exe_command(export_shell)


class MyImport(Shell):
    """
    从sql文件导入
    """

    def __init__(self, mongo: Mongo):
        super().__init__()
        self.mongo = mongo

    def import_db(self, database, db_dir):
        """
        读取mongo导出的数据库目录文件并导入到mongo中
        :param db_dir: 数据库目录
        :return:
        """
        import_shell = f'{mongorestore_exe} -h {self.mongo.db_host}:{self.mongo.db_port} --username={self.mongo.db_user} --password="{self.mongo.db_pass}" --drop -d {database} {db_dir}'
        self._exe_command(import_shell)
        pass

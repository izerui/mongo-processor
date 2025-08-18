import os
import platform
from subprocess import Popen, PIPE, STDOUT

if platform.system() == 'Windows':
    mongodump_exe = os.path.join('mongodb-database-tools', 'windows-x86_64-100.13.0', 'mongodump.exe')
    mongorestore_exe = os.path.join('mongodb-database-tools', 'windows-x86_64-100.13.0', 'mongorestore.exe')
elif platform.system() == 'Linux':
    mongodump_exe = os.path.join('mongodb-database-tools', 'rhel93-x86_64-100.13.0', 'mongodump')
    mongorestore_exe = os.path.join('mongodb-database-tools', 'rhel93-x86_64-100.13.0', 'mongorestore')
elif platform.system() == 'Darwin':
    mongodump_exe = os.path.join('mongodb-database-tools', 'macos-arm64-100.13.0', 'mongodump')
    mongorestore_exe = os.path.join('mongodb-database-tools', 'macos-arm64-100.13.0', 'mongorestore')


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
                try:
                    print(line.decode().strip())
                except:
                    print(str(line))
        exitcode = process.wait()
        if exitcode != 0:
            raise BaseException('命令执行失败')
        return process, exitcode


class Mongo:
    __slots__ = ['host', 'port', 'username', 'password']

    def __init__(self, db_host, db_port, db_user, db_pass):
        self.host = db_host
        self.port = db_port
        self.username = db_user
        self.password = db_pass


class MyDump(Shell):
    """
    导出数据库备份到目录
    """

    def __init__(self, mongo: Mongo, parallelNum: int = 4):
        super().__init__()
        self.mongo = mongo
        self.parallelNum = parallelNum

    def export_db(self, database, dump_root_path):
        """
        导出mongo数据库到dump目录
        :param database: 数据库名
        :param dump_root_path: dump根目录,会自动创建对应数据库名的dump目录
        :return:
        """
        auth_append = f'--username={self.mongo.username} --password="{self.mongo.password}" --authenticationDatabase=admin' if self.mongo.username else ''
        export_shell = f'''{mongodump_exe} --host="{self.mongo.host}:{self.mongo.port}" --db={database} --out={dump_root_path} --numParallelCollections {self.parallelNum} --gzip {auth_append}'''
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
        user_append = f'--username="{self.mongo.username}"' if self.mongo.username else ''
        password_append = f'--password="{self.mongo.password}"' if self.mongo.password else ''
        auth_database_append = f'--authenticationDatabase=admin' if self.mongo.username else ''
        import_shell = f'{mongorestore_exe} --host="{self.mongo.host}:{self.mongo.port}" {user_append} {password_append} {auth_database_append} --drop --gzip --db="{database}" {db_dir}'
        self._exe_command(import_shell)
        pass

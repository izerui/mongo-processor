import os
import shutil
from configparser import ConfigParser

from dump import MyDump, MyImport, Mongo

if __name__ == "__main__":
    config = ConfigParser()
    config.read('config.ini')
    source = Mongo(config.get('source', 'host'), config.get('source', 'port'), config.get('source', 'username'),
                   config.get('source', 'password'))
    target = Mongo(config.get('target', 'host'), config.get('target', 'port'), config.get('target', 'username'),
                   config.get('target', 'password'))
    databases = config.get('global', 'databases').split(',')
    parallelNum = config.getint('global', 'parallel')
    dump_folder = 'dumps'
    if not os.path.exists(dump_folder):
        os.makedirs(dump_folder)
    for db in databases:
        # 导出生产mongo库
        print(f' ℹ️从{source.host}导出: {db}')
        mydump = MyDump(source, parallelNum)
        mydump.export_db(db, dump_folder)
        print(f' ✅成功 从{source.host}导出: {db}')
        db_dir = os.path.join(dump_folder, db)
        # 导入uat
        print(f' ℹ️导入{target.host}: {db}')
        myimport = MyImport(target)
        myimport.import_db(db, db_dir)
        print(f' ✅成功 导入{target.host}: {db}')

        # 删除导出的文件
        print(f' ✅删除临时sql文件缓存: {db_dir}')
        shutil.rmtree(db_dir)


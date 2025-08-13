import os
import shutil
from configparser import ConfigParser

from dump import MyDump, MyImport, Mongo

if __name__ == "__main__":
    config = ConfigParser()
    config.read('config.ini')
    source = Mongo(config.get('source', 'db_host'), config.get('source', 'db_port'), config.get('source', 'db_user'),
                   config.get('source', 'db_pass'))
    target = Mongo(config.get('target', 'db_host'), config.get('target', 'db_port'), config.get('target', 'db_user'),
                   config.get('target', 'db_pass'))
    databases = config.get('global', 'databases').split(',')
    dump_folder = 'dumps'
    # if not os.path.exists(dump_folder):
    #     os.makedirs(dump_folder)
    # for db in databases:
    #     # 导出生产mongo库
    #     print(f'---------------------------------------------> 从{source.db_host}导出: {db}')
    #     mydump = MyDump(source)
    #     mydump.export_db(db, dump_folder)
    #     print(f'---------------------------------------------> 成功 从{source.db_host}导出: {db}')

    for db in databases:
        db_dir = os.path.join(dump_folder, db)
        # 导入uat
        print(f'---------------------------------------------> 导入{target.db_host}: {db}')
        myimport = MyImport(target)
        myimport.import_db(db, db_dir)
        print(f'---------------------------------------------> 成功 导入{target.db_host}: {db}')

        # 删除导出的文件
        print(f'--------------------------------------------->> 删除临时sql文件缓存: {db_dir}')
        shutil.rmtree(db_dir)

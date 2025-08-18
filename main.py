import os
import shutil
import sys
import time
from configparser import ConfigParser
from pathlib import Path

from dump import MyDump, MyImport, Mongo

def cleanup_dump_folder(dump_folder: Path) -> None:
    """清理历史导出目录"""
    if dump_folder.exists():
        import shutil
        # 只删除目录内容，不删除目录本身（云盘挂载路径）
        for item in dump_folder.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)

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
    # 设置导出目录
    dump_folder = Path(__file__).parent / 'dumps'
    # 清理历史导出目录,如果配置不导出则不清理
    cleanup_dump_folder(dump_folder)
    dump_folder.mkdir(exist_ok=True)
    print(f"⚙️  导出配置: 并发数={parallelNum}")
    total_start_time = time.time()
    for db in databases:
        # 导出生产mongo库
        print(f' ℹ️从{source.host}导出: {db}')
        export_start_time = time.time()
        mydump = MyDump(source, parallelNum)
        mydump.export_db(db, dump_folder)
        export_time = time.time() - export_start_time
        print(f' ✅成功 从{source.host}导出: {db} (耗时: {export_time:.2f}秒)')

        db_dir = os.path.join(dump_folder, db)
        # 导入uat
        print(f' ℹ️导入{target.host}: {db}')
        import_start_time = time.time()
        myimport = MyImport(target)
        myimport.import_db(db, db_dir)
        import_time = time.time() - import_start_time
        print(f' ✅成功 导入{target.host}: {db} (耗时: {import_time:.2f}秒)')

        # 删除导出的文件
        print(f' ✅删除临时sql文件缓存: {db_dir}')
        shutil.rmtree(db_dir)

    total_time = time.time() - total_start_time
    print(f' 🎉所有数据库操作完成，总耗时: {total_time:.2f}秒')

    # 程序结束
    print("💤 程序执行完成，进入休眠状态...")

    try:
        while True:
            time.sleep(3600)  # 每小时检查一次
    except KeyboardInterrupt:
        print("收到退出信号，程序结束")
        sys.exit(0)

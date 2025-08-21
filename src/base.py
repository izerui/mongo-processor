import os
import platform
import subprocess
import sys
import time
import threading
from datetime import datetime
from subprocess import TimeoutExpired
from typing import Optional, Dict, Any


class Shell(object):

    def _exe_command(self, command, timeout=300, debug=False, show_timestamp=True):
        """
        执行命令并实时输出日志，支持时间戳和进度显示
        :param command: shell 命令
        :param timeout: 超时时间（秒）
        :param debug: 是否显示调试信息
        :param show_timestamp: 是否在每行输出前添加时间戳
        :return: result, exitcode
        """
        start_time = time.time()
        start_datetime = datetime.now()

        print(f"🚀 开始执行: {command}")
        def kill_process(p):
            """超时后杀死进程"""
            try:
                p.kill()
                print(f"⏰ [{datetime.now().strftime('%H:%M:%S')}] 进程已被强制终止")
            except:
                pass

        try:
            # 使用Popen实现实时输出，合并stdout和stderr
            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # 合并错误输出到标准输出
                text=True,
                bufsize=1,
                universal_newlines=True
            )

            # 设置超时定时器
            timer = threading.Timer(timeout, kill_process, [process])
            if timeout > 0:
                timer.start()

            output_lines = []

            try:
                # 实时输出，支持时间戳
                if process.stdout:
                    for line in iter(process.stdout.readline, ''):
                        if line:
                            line = line.rstrip()
                            output_lines.append(line)

                            if show_timestamp:
                                timestamp = datetime.now().strftime('%H:%M:%S')
                                formatted_line = f"[{timestamp}] {line}"
                            else:
                                formatted_line = line

                            print(formatted_line)
                            sys.stdout.flush()

                # 等待进程完成
                process.wait()

            finally:
                # 取消定时器
                if timeout > 0:
                    timer.cancel()

            elapsed = time.time() - start_time
            end_datetime = datetime.now()

            # 检查是否超时
            if process.returncode is None or process.returncode < 0:
                print(f"⏰ [{end_datetime.strftime('%H:%M:%S')}] 命令执行超时({timeout}秒)，实际耗时: {elapsed:.2f}秒")
                raise BaseException(f"命令执行超时({timeout}秒)")

            # 检查返回码
            if process.returncode != 0:
                # 收集错误输出
                error_output = "\n".join(output_lines[-10:]) if output_lines else "无错误输出"
                raise BaseException(f'命令执行失败 (exit code: {process.returncode})\n错误详情: {error_output}')

            return process, process.returncode

        except KeyboardInterrupt:
            print(f"\n🛑 [{datetime.now().strftime('%H:%M:%S')}] 用户中断执行")
            if 'process' in locals() and process:
                process.terminate()
                process.wait()
            raise BaseException("用户中断执行")

        except Exception as e:
            elapsed = time.time() - start_time
            print(f"❌ [{datetime.now().strftime('%H:%M:%S')}] 命令执行异常: {e}")
            raise BaseException(f'命令执行失败: {e}')


class Mongo:
    __slots__ = ['host', 'port', 'username', 'password']

    def __init__(self, db_host, db_port, db_user, db_pass):
        self.host = db_host
        self.port = db_port
        self.username = db_user
        self.password = db_pass


class ShardConfig:
    """分片配置类"""

    def __init__(self):
        self.min_documents_for_shard = 1000000  # 分片最小文档数
        self.default_shard_count = 4  # 默认分片数
        self.max_shard_count = 16  # 最大分片数




class ObjectIdRange:
    """ObjectId范围类"""

    def __init__(self, start_id=None, end_id=None):
        self.start_id = start_id
        self.end_id = end_id

    def to_query(self) -> Optional[Dict[str, Any]]:
        """转换为MongoDB查询条件"""
        if not self.start_id and not self.end_id:
            return None

        query = {}
        if self.start_id and self.end_id:
            query['_id'] = {'$gte': self.start_id, '$lt': self.end_id}
        elif self.start_id:
            query['_id'] = {'$gte': self.start_id}
        elif self.end_id:
            query['_id'] = {'$lt': self.end_id}

        return query

    def __str__(self):
        return f"ObjectIdRange({self.start_id}, {self.end_id})"


# 获取当前脚本目录（src目录）
script_dir = os.path.dirname(os.path.abspath(__file__))
# 获取项目根目录（包含mongodb-database-tools的目录）
base_dir = os.path.dirname(script_dir)

if platform.system() == 'Windows':
    mongodump_exe = os.path.join(base_dir, 'mongodb-database-tools', 'windows-x86_64-100.13.0', 'mongodump.exe')
    mongorestore_exe = os.path.join(base_dir, 'mongodb-database-tools', 'windows-x86_64-100.13.0', 'mongorestore.exe')
elif platform.system() == 'Linux':
    mongodump_exe = os.path.join(base_dir, 'mongodb-database-tools', 'rhel93-x86_64-100.13.0', 'mongodump')
    mongorestore_exe = os.path.join(base_dir, 'mongodb-database-tools', 'rhel93-x86_64-100.13.0', 'mongorestore')
elif platform.system() == 'Darwin':
    mongodump_exe = os.path.join(base_dir, 'mongodb-database-tools', 'macos-arm64-100.13.0', 'mongodump')
    mongorestore_exe = os.path.join(base_dir, 'mongodb-database-tools', 'macos-arm64-100.13.0', 'mongorestore')

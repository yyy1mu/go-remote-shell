# File: tools/replay.py

import argparse
import json
import base64
import sys
import time
import re

def replay_session(log_file_path, output_file_path=None, delay=0.01):
    """
    读取统一格式的会话日志，并只回放 [OUT] 部分来重现终端或保存到文件。

    :param log_file_path: 要回放的日志文件路径。
    :param output_file_path: (可选) 保存原始终端流的输出文件路径。
    :param delay: 在终端回放时的延迟（秒）。
    """
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"错误: 文件未找到: {log_file_path}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"错误: 无法读取文件: {e}", file=sys.stderr)
        sys.exit(1)

    output_stream = None
    is_file_output = output_file_path is not None

    try:
        if is_file_output:
            # 如果是文件输出，以二进制写入模式打开文件
            output_stream = open(output_file_path, 'wb')
            print(f"--- 正在转换日志并保存到: {output_file_path} ---")
        else:
            # 如果是终端回放，使用标准输出的二进制缓冲区
            output_stream = sys.stdout.buffer
            print(f"\n--- 开始在终端回放会话: {log_file_path} ---")
            time.sleep(1)
            # 清空屏幕，准备回放
            output_stream.write(b'\x1b[2J\x1b[H')
            output_stream.flush()

        # 匹配 [TIMESTAMP] [OUT] BASE64_PAYLOAD
        line_pattern = re.compile(r"\[.*?] \[OUT\] (.*)")

        for line in lines:
            match = line_pattern.match(line)
            if not match:
                continue # 我们只关心 OUT 方向的数据

            b64_encoded_json = match.group(1)

            try:
                # 第 1 层解码: 从 Base64 得到原始 WebSocket 消息 (JSON)
                json_bytes = base64.b64decode(b64_encoded_json)

                # 解析 JSON
                msg_obj = json.loads(json_bytes)

                # 检查是否为包含终端数据的 'data' 消息
                if msg_obj.get('type') == 'data' and 'payload' in msg_obj:
                    # 第 2 层解码: 从 payload 字段得到最终的原始终端数据
                    raw_pty_data = base64.b64decode(msg_obj['payload'])

                    # 将解码后的原始二进制数据写入输出流
                    output_stream.write(raw_pty_data)

                    if not is_file_output:
                        output_stream.flush() # 立即刷新缓冲区以在终端显示
                        time.sleep(delay)   # 模拟实时延迟

            except (json.JSONDecodeError, KeyError, TypeError, Exception):
                # 忽略任何无法解析的行，例如 FIDO2 握手消息
                continue

    except IOError as e:
        print(f"错误: 写入输出时发生错误: {e}", file=sys.stderr)
    finally:
        if is_file_output and output_stream:
            output_stream.close()
            print(f"--- 成功将会话原始流保存到: {output_file_path} ---")
        elif not is_file_output:
             print("\n\n--- 会话回放结束 ---")


def main():
    parser = argparse.ArgumentParser(description="回放或转换 Go Remote Shell 的会话日志。")
    parser.add_argument("logfile", help="要处理的统一会话日志文件路径。")
    parser.add_argument(
        "-d", "--delay",
        type=float,
        default=0.01,
        help="在终端回放时的延迟（秒），默认为 0.01"
    )
    parser.add_argument(
        "-o", "--output",
        metavar="FILE",
        help="将原始终端流输出到指定文件，而不是在终端上回放"
    )
    args = parser.parse_args()
    replay_session(args.logfile, args.output, args.delay)


if __name__ == "__main__":
    main()
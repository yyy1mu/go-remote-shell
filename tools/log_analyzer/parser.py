# File: tools/log_analyzer/parser.py

import re
import base64
import json
from datetime import datetime
import pyte

class LogParser:
    def __init__(self, log_file_path):
        self.file_path = log_file_path
        self.events = []
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                header_passed = False
                for line in f:
                    if not header_passed:
                        if line.strip() == "---------------------":
                            header_passed = True
                        continue

                    # 匹配 [TIMESTAMP] [DIRECTION] BASE64_PAYLOAD
                    match = re.match(r"\[(.*?)\] \[(IN|OUT)\] (.*)", line)
                    if match:
                        timestamp_str, direction, b64_encoded_json = match.groups()

                        # ==================== 关键修复 ====================
                        # 我们只关心从 Agent -> UI 的输出流，因为它包含了完整的终端展现
                        if direction == 'OUT':
                            try:
                                # 第 1 层解码：从 Base64 解码得到 JSON 字符串
                                json_bytes = base64.b64decode(b64_encoded_json)

                                # 解析 JSON
                                msg_obj = json.loads(json_bytes)

                                # 如果是包含终端数据的 'data' 类型的消息
                                if msg_obj.get('type') == 'data' and 'payload' in msg_obj:
                                    # 第 2 层解码：从 payload 字段中解码得到最终的原始终端数据
                                    raw_pty_data = base64.b64decode(msg_obj['payload'])

                                    # 解析时间戳并添加到事件列表
                                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                                    self.events.append((timestamp, raw_pty_data))

                            except (json.JSONDecodeError, TypeError, Exception):
                                # 忽略任何解析失败的行（例如 FIDO2 握手消息）
                                continue
                        # ===============================================

            self.events.sort(key=lambda x: x[0])
        except FileNotFoundError:
            raise ValueError(f"错误: 日志文件未找到: {log_file_path}")
        except Exception as e:
            raise IOError(f"错误: 无法读取文件: {e}")

        self._clean_transcript = None
        self._metadata = None

    def get_metadata(self):
        if self._metadata is not None:
            return self._metadata
        metadata = {}
        with open(self.file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            patterns = { 'Time': r"Time: (.*)", 'ClientID': r"ClientID: (.*)", 'User': r"User: (.*)", 'SessionID': r"SessionID: (.*)" }
            for key, pattern in patterns.items():
                match = re.search(pattern, content)
                if match: metadata[key] = match.group(1).strip()
        self._metadata = metadata
        return self._metadata

    def generate_clean_transcript(self):
        """
        使用 pyte 终端模拟器生成一份完美的、干净的会话文本记录。
        """
        if self._clean_transcript is not None:
            return self._clean_transcript

        screen = pyte.Screen(80, 24, history=10000)
        stream = pyte.ByteStream(screen)

        # self.events 现在只包含解码后的原始终端数据
        for _, data in self.events:
            stream.feed(data)

        history_lines = list(screen.history)
        history_lines.extend(screen.display)

        self._clean_transcript = "\n".join(line.rstrip() for line in history_lines)

        return self._clean_transcript
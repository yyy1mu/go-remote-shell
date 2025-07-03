# File: tools/log_analyzer/parser.py

import re
import base64
import pyte
from datetime import datetime
import collections.abc # 导入 collections.abc 以进行可迭代性检查

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
                    match = re.match(r"\[(.*?)\] \[(IN|OUT)\] (.*)", line)
                    if match:
                        timestamp_str, _, payload_b64 = match.groups()
                        try:
                            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                            data = base64.b64decode(payload_b64)
                            self.events.append((timestamp, data))
                        except (ValueError, TypeError):
                            continue
            self.events.sort(key=lambda x: x[0])
        except FileNotFoundError:
            raise ValueError(f"错误: 日志文件未找到: {log_file_path}")
        except Exception as e:
            raise IOError(f"错误: 无法读取文件: {e}")

        self._clean_transcript = None
        self._metadata = None
        self._commands = None

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

        screen = pyte.Screen(80, 24)
        stream = pyte.Stream(screen)
        with open(self.file_path, 'rb') as f:
            stream.feed(f.read().decode('utf-8'))
            # 获取渲染后的文本
            cleaned_text = '\n'.join(screen.display)
            self._clean_transcript = "".join(cleaned_text)

        return self._clean_transcript


    def extract_commands(self):
        """
        从干净的会话文本中提取用户输入的命令。
        """
        if self._commands is not None:
            return self._commands

        transcript = self.generate_clean_transcript()
        commands = []
        prompt_pattern = re.compile(r".*?(\$|#)\s+(.*)")

        for line in transcript.splitlines():
            line = line.strip()
            if not line:
                continue

            match = prompt_pattern.match(line)
            if match:
                command = match.group(2).strip()
                if command:
                    commands.append(command)

        self._commands = commands
        return self._commands
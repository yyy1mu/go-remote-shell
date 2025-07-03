# File: tools/log_analyzer/llm_analyzer.py

import os
import requests
import json

class DeepSeekAnalyzer:
    API_URL = "https://api.deepseek.com/chat/completions"

    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        if not self.api_key:
            raise ValueError("错误: 未找到 DEEPSEEK_API_KEY。请设置环境变量或传入密钥。")
        self.headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"}

    def analyze_transcript(self, transcript_text, metadata):
        """
        将完整的会话文本记录发送给 DeepSeek API 进行分析。
        """
        print("\n正在将会话文本发送给 DeepSeek 进行智能分析，请稍候...")

        system_prompt = (
            "你是一名资深的Linux系统管理员和网络安全专家。"
            "你的任务是深入分析以下终端会话的完整文本记录，找出任何值得注意的行为。"
        )
        user_prompt = (
            "请仔细分析下面的终端会话文本记录。这份记录完美地还原了用户屏幕上显示的所有内容。\n"
            "会话的元数据如下：\n"
            f"- 用户: {metadata.get('User', 'N/A')}\n"
            f"- 时间: {metadata.get('Time', 'N/A')}\n"
            f"- 客户端ID: {metadata.get('ClientID', 'N/A')}\n\n"
            "请基于这份完整的会话文本，提供以下几点分析：\n"
            "1. **行为总结**: 简要概括该用户在此次会话中从头到尾完成了什么主要任务。\n"
            "2. **潜在风险识别**: 识别任何可能存在的安全风险、危险操作（如 `rm -rf`、修改权限、错误的 `curl` 用法等）、或不规范的操作习惯。\n"
            "3. **命令与输出分析**: 根据上下文，分析几个关键命令及其输出，解释其意图和结果。\n"
            "4. **综合评价**: 给出一个总体评价，例如“常规系统检查”、“高危的配置变更”或“疑似的渗透测试行为”。\n\n"
            "请使用清晰的 Markdown 格式输出你的分析结果。\n\n"
            "--- 完整会话文本记录开始 ---\n"
            f"{transcript_text}"
            "\n--- 完整会话文本记录结束 ---"
        )

        payload = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.2,
            "stream": False
        }

        try:
            response = requests.post(self.API_URL, headers=self.headers, data=json.dumps(payload), timeout=90)
            response.raise_for_status()
            result = response.json()
            analysis = result['choices'][0]['message']['content']
            return analysis
        except requests.exceptions.RequestException as e:
            return f"错误: 调用 DeepSeek API 失败: {e}"
        except (KeyError, IndexError) as e:
            return f"错误: 解析 DeepSeek API 响应失败: {e}\n响应内容: {response.text}"
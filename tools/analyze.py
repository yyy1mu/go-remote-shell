# File: tools/analyze.py

import argparse
from log_analyzer.parser import LogParser
from log_analyzer.llm_analyzer import DeepSeekAnalyzer

def main():
    """
    主函数，读取日志，生成干净文本，并交给 DeepSeek 分析。
    """
    parser = argparse.ArgumentParser(description="使用 DeepSeek 智能分析 Go Remote Shell 的会话日志")
    parser.add_argument("logfile", help="要分析的会话日志文件路径")
    args = parser.parse_args()

    try:
        # 1. 初始化解析器
        print(f"正在解析日志文件: {args.logfile} ...")
        log_parser = LogParser(args.logfile)

        # 2. 生成最干净的会话文本记录
        transcript = log_parser.generate_clean_transcript()
        if not transcript.strip():
            print("错误：解析出的会话内容为空，请检查日志文件和 pyte 库是否正常工作。")
            return

        print(transcript)
        print("日志解析完成，已生成干净的会话文本。")

        # 3. 初始化 AI 分析器
        analyzer = DeepSeekAnalyzer()

        # 4. 获取元数据并调用 AI 分析
        metadata = log_parser.get_metadata()
        ai_analysis = analyzer.analyze_transcript(transcript, metadata)

        # 5. 打印最终结果
        print("\n========================================")
        print("      DeepSeek 智能分析报告")
        print("========================================")
        print(ai_analysis)

    except (ValueError, IOError) as e:
        print(e)
    except Exception as e:
        print(f"发生未知错误: {e}")

if __name__ == "__main__":
    main()
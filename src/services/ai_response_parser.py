"""
AI 响应解析工具
"""
import json
from typing import Any


class EmptyAIResponseError(ValueError):
    """AI 返回了空内容。"""


def extract_ai_response_content(response: Any) -> str:
    """从不同形态的 AI 响应中提取文本内容。"""
    if response is None:
        raise EmptyAIResponseError("AI响应对象为空。")

    if isinstance(response, (bytes, bytearray)):
        text = response.decode("utf-8", errors="replace")
        return _normalize_text_content(text)

    if isinstance(response, str):
        return _normalize_text_content(response)

    output_text = getattr(response, "output_text", None)
    if isinstance(output_text, str):
        return _normalize_text_content(output_text)

    choices = getattr(response, "choices", None)
    if choices:
        message = getattr(choices[0], "message", None)
        if message is None:
            raise EmptyAIResponseError("AI响应缺少 message。")
        content = getattr(message, "content", None)
        
        # 智谱等 OpenAI 兼容网关在某些模式下会把输出放在 reasoning_content 而非 content
        try:
            return _normalize_text_content(_coerce_content_parts(content))
        except EmptyAIResponseError:
            reasoning_content = getattr(message, "reasoning_content", None)
            if reasoning_content:
                return _normalize_text_content(_coerce_content_parts(reasoning_content))
            raise

    raise ValueError(f"无法识别的AI响应类型: {type(response).__name__}")


def parse_ai_response_json(content: str) -> dict:
    """解析 AI 文本响应中的 JSON。"""
    cleaned = _strip_code_fences(content)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as exc:
        return _extract_first_json_value(cleaned, exc)


def _coerce_content_parts(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, (bytes, bytearray)):
        return content.decode("utf-8", errors="replace")
    if not isinstance(content, list):
        raise ValueError(f"AI响应内容类型不受支持: {type(content).__name__}")

    parts: list[str] = []
    for item in content:
        if isinstance(item, str):
            parts.append(item)
            continue
        if isinstance(item, dict):
            text = item.get("text")
            if isinstance(text, str):
                parts.append(text)
            continue
        text = getattr(item, "text", None)
        if isinstance(text, str):
            parts.append(text)
    return "".join(parts)


def _normalize_text_content(content: str) -> str:
    text = str(content).strip()
    if not text:
        raise EmptyAIResponseError("AI响应内容为空。")
    return text


def _strip_code_fences(content: str) -> str:
    cleaned = content.strip()
    if cleaned.startswith("```json"):
        cleaned = cleaned[7:]
    if cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    return cleaned.strip()


def _extract_first_json_value(
    content: str,
    fallback_error: json.JSONDecodeError,
):
    """整体解析失败时，尝试定位文本中的 JSON 值。

    两种情况必须区分对待：

    1. 文本以 JSON 起始符开头（模型直接给 JSON，或给了多个拼接的 JSON 对象）：
       只接受**从开头就能解析成功**的结果。若开头解析失败，说明这份 JSON 本身
       残缺——典型原因是输出被 max_tokens 截断。
       此时**绝不能**退而求其次去解析内层片段：那会把残缺 JSON 的某个子对象
       （例如 criteria_analysis 里的嵌套对象）当成顶层结果返回，制造出
       「响应缺少必需字段 'is_recommended'」的假象，把「截断」误报成「模型漏字段」，
       既误导排查方向，也白白浪费重试次数。

    2. 文本以说明性文字开头（模型在 JSON 前后加了额外文字）：
       跳过前缀，寻找第一个能完整解析的 JSON 值。
    """
    decoder = json.JSONDecoder()
    stripped = content.lstrip()

    if stripped[:1] in "{[":
        # 开头即 JSON：成功就用它（兼容多个 JSON 对象拼接，raw_decode 会取第一个），
        # 失败则如实抛出——这是截断，不是「字段缺失」。
        parsed, _ = decoder.raw_decode(stripped)
        return parsed

    last_error: json.JSONDecodeError | None = None

    for start_index, char in enumerate(content):
        if char not in "{[":
            continue
        try:
            parsed, _ = decoder.raw_decode(content[start_index:])
            return parsed
        except json.JSONDecodeError as exc:
            last_error = exc

    if last_error is not None:
        raise last_error
    raise fallback_error

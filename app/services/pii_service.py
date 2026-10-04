import re
from typing import Any, Dict, List, Tuple

CREDIT_CARD_REGEX = re.compile(r"\b(?:\d{4}[-\s]?){3}\d{4}\b")
SSN_REGEX = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
PHONE_REGEX = re.compile(r"\b(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b")

SENSITIVE_KEY_PATTERNS = {
    "password",
    "secret",
    "token",
    "api_key",
    "apikey",
    "access_token",
    "auth_token",
    "refresh_token",
    "cvv",
    "ssn",
    "social_security",
    "pin",
    "private_key",
}


class PIIRedactionService:
    @classmethod
    def redact_string(cls, text: str) -> Tuple[str, int]:
        count = 0
        if CREDIT_CARD_REGEX.search(text):
            text, c = CREDIT_CARD_REGEX.subn("[REDACTED_CREDIT_CARD]", text)
            count += c
        if SSN_REGEX.search(text):
            text, c = SSN_REGEX.subn("[REDACTED_SSN]", text)
            count += c
        if PHONE_REGEX.search(text):
            text, c = PHONE_REGEX.subn("[REDACTED_PHONE]", text)
            count += c
        return text, count

    @classmethod
    def redact_payload(cls, data: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
        total_redacted = 0
        sanitized: Dict[str, Any] = {}

        for key, val in data.items():
            lower_key = key.lower()
            if any(sens in lower_key for sens in SENSITIVE_KEY_PATTERNS):
                sanitized[key] = "[REDACTED_SECRET]"
                total_redacted += 1
            elif isinstance(val, dict):
                sub_dict, c = cls.redact_payload(val)
                sanitized[key] = sub_dict
                total_redacted += c
            elif isinstance(val, list):
                sanitized_list = []
                for item in val:
                    if isinstance(item, dict):
                        sub_dict, c = cls.redact_payload(item)
                        sanitized_list.append(sub_dict)
                        total_redacted += c
                    elif isinstance(item, str):
                        s, c = cls.redact_string(item)
                        sanitized_list.append(s)
                        total_redacted += c
                    else:
                        sanitized_list.append(item)
                sanitized[key] = sanitized_list
            elif isinstance(val, str):
                s, c = cls.redact_string(val)
                sanitized[key] = s
                total_redacted += c
            else:
                sanitized[key] = val

        return sanitized, total_redacted

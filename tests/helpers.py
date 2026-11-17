import html
import json
from pathlib import Path

import requests

FIXTURES = Path(__file__).parent / "fixtures"


def response(status, text="", headers=None):
    result = requests.Response()
    result.status_code = status
    result._content = text.encode("utf-8")
    result.encoding = "utf-8"
    result.headers["Content-Type"] = "text/javascript; charset=utf-8"
    result.headers.update(headers or {})
    return result


def widget(deck_id=1, deck_text="60 Island\nsideboard\n15 Dispel", format_name="pauper"):
    markup = (
        '<input name="deck_input[format]" value="' + html.escape(format_name, quote=True) + '">'
        '<input name="deck_input[deck]" value="' + html.escape(deck_text, quote=True) + '">'
    )
    return (
        f'document.querySelector(".widget-deck-placeholder[data-id=\'{deck_id}\']");'
        f"elem.innerHTML = {json.dumps(markup, ensure_ascii=False)};"
    )

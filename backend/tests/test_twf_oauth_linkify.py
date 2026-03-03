import os
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = BACKEND_ROOT.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("TWF_BASE", "https://example.com")
os.environ.setdefault("TWF_CLIENT_ID", "client-id")
os.environ.setdefault("TWF_CLIENT_SECRET", "client-secret")
os.environ.setdefault("TWF_REDIRECT_URI", "https://example.com/callback")
os.environ.setdefault("FRONTEND_RETURN", "https://example.com/app")
os.environ.setdefault("TOKEN_DB_PATH", "/tmp/twf_test_tokens.sqlite3")
os.environ.setdefault("TOKEN_ENC_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")

from app.auth import twf_oauth


def test_plain_text_to_ips_html_linkifies_permalink() -> None:
    content = "Permalink: https://example.com/viewer?model=gfs&run=20260303_00z."
    rendered = twf_oauth._plain_text_to_ips_html(content)
    assert (
        '<a href="https://example.com/viewer?model=gfs&amp;run=20260303_00z" '
        'rel="nofollow noopener" target="_blank">'
        "https://example.com/viewer?model=gfs&amp;run=20260303_00z"
        "</a>."
    ) in rendered


def test_plain_text_to_ips_html_escapes_script_markup() -> None:
    content = '<script>alert("xss")</script> https://example.com'
    rendered = twf_oauth._plain_text_to_ips_html(content)
    assert "<script>" not in rendered
    assert "&lt;script&gt;alert(" in rendered
    assert '<a href="https://example.com"' in rendered


def test_plain_text_to_ips_html_preserves_line_breaks_as_br() -> None:
    content = "first line\nsecond line\r\nthird line"
    rendered = twf_oauth._plain_text_to_ips_html(content)
    assert rendered == "first line<br>second line<br>third line"

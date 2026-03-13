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


def test_build_twf_share_html_renders_summary_image_and_permalink() -> None:
    rendered = twf_oauth.build_twf_share_html(
        summary='HRRR <b>Snow</b> outlook',
        permalink="https://theweathermodels.com/viewer?model=hrrr&run=20260308_00z",
        image_url="https://cdn.theweathermodels.com/share/2026/03/08/example.png",
    )

    assert rendered == (
        "HRRR &lt;b&gt;Snow&lt;/b&gt; outlook"
        "<br><br>"
        '<img src="https://cdn.theweathermodels.com/share/2026/03/08/example.png" alt="Model screenshot">'
        "<br><br>"
        '<a href="https://theweathermodels.com/viewer?model=hrrr&amp;run=20260308_00z" '
        'rel="nofollow noopener" target="_blank">'
        "View map on CartoSky"
        "</a>"
    )


def test_build_twf_share_html_omits_image_block_when_not_provided() -> None:
    rendered = twf_oauth.build_twf_share_html(
        summary="GFS update",
        permalink="https://theweathermodels.com/viewer?model=gfs",
    )

    assert '<img src="' not in rendered
    assert rendered == (
        "GFS update"
        "<br><br>"
        '<a href="https://theweathermodels.com/viewer?model=gfs" rel="nofollow noopener" target="_blank">'
        "View map on CartoSky"
        "</a>"
    )


def test_build_twf_share_html_rejects_non_http_urls() -> None:
    try:
        twf_oauth.build_twf_share_html(
            summary="Bad link",
            permalink="javascript:alert(1)",
        )
    except ValueError as exc:
        assert str(exc) == "Permalink must be an absolute http(s) URL."
    else:
        raise AssertionError("Expected ValueError for invalid permalink")

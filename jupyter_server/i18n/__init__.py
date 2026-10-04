"""项目内部接口说明。"""

from __future__ import annotations

import errno
import json
import re
from collections import defaultdict
from os.path import dirname
from os.path import join as pjoin
from typing import Any

I18N_DIR = dirname(__file__)
# Cache structure:
# {'nbjs': {   # Domain
#   'zh-CN': {  # Language code
#     <english string>: <translated string>
#     ...
#   }
# }}
TRANSLATIONS_CACHE: dict[str, Any] = {"nbjs": {}}


_accept_lang_re = re.compile(
    r"""
(?P<lang>[a-zA-Z]{1,8}(-[a-zA-Z]{1,8})?)
(\s*;\s*q\s*=\s*
  (?P<qvalue>[01](.\d+)?)
)?""",
    re.VERBOSE,
)


def parse_accept_lang_header(accept_lang):
    """项目内部接口说明。"""
    by_q = defaultdict(list)
    for part in accept_lang.split(","):
        m = _accept_lang_re.match(part.strip())
        if not m:
            continue
        lang, qvalue = m.group("lang", "qvalue")
        # Browser header format is zh-CN, gettext uses zh_CN
        lang = lang.replace("-", "_")
        qvalue = 1.0 if qvalue is None else float(qvalue)
        if qvalue == 0:
            continue  # 0 means not accepted
        by_q[qvalue].append(lang)

    res = []
    for _, langs in sorted(by_q.items()):
        res.extend(sorted(langs))
    return res


def load(language, domain="nbjs"):
    """项目内部接口说明。"""
    try:
        f = open(pjoin(I18N_DIR, language, "LC_MESSAGES", "nbjs.json"), encoding="utf-8")  # noqa: SIM115
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
        return {}

    with f:
        data = json.load(f)
    return data["locale_data"][domain]


def cached_load(language, domain="nbjs"):
    """项目内部接口说明。"""
    domain_cache = TRANSLATIONS_CACHE[domain]
    try:
        return domain_cache[language]
    except KeyError:
        data = load(language, domain)
        domain_cache[language] = data
        return data


def combine_translations(accept_language, domain="nbjs"):
    """项目内部接口说明。"""
    lang_codes = parse_accept_lang_header(accept_language)
    combined: dict[str, Any] = {}
    for language in lang_codes:
        if language == "en":
            # en is default, all translations are in frontend.
            combined.clear()
        else:
            combined.update(cached_load(language, domain))

    combined[""] = {"domain": "nbjs"}

    return {"domain": domain, "locale_data": {domain: combined}}

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
청바지(청년혜택은 바로 지금) - 청년정책 데이터 수집 파이프라인
===========================================================

온통청년 오픈API + 지자체(서울시 청년몽땅정보통 / 인천시 청년포털) 공고문을
requests + BeautifulSoup으로 수집하고, 비정형 텍스트에서 정규표현식(re)으로
나이 / 소득 / 마감일 / 구비서류를 뽑아 정제한 뒤 Supabase(Policy, Policy_Document)
에 upsert 합니다.

------------------------------------------------------------------
※ 스키마 관련 중요 안내
------------------------------------------------------------------
이 스크립트는 "Policy / Policy_Document" 테이블이 아래와 같은 컬럼을 가진다고
가정하고 작성되었습니다 (기획서에 명시된 7개 테이블 중 2개).
실제 Supabase에서 만든 컬럼명이 다르다면, 파일을 전체 수정할 필요 없이
아래 POLICY_COLUMNS / POLICY_DOCUMENT_COLUMNS 매핑값만 실제 컬럼명으로
바꿔주면 됩니다.

    Policy
      policy_id            text/varchar (PK, 예: 'ontong-R2024030720423')
      source               text  ('ontong_youth' | 'seoul_youth_portal' | 'incheon_youth_portal')
      title                text  (정책명)
      category             text  (정책 분류)
      organization         text  (주관/운영 기관)
      description          text  (정책 설명)
      support_content      text  (지원 내용)
      min_age              int4  (최소 연령)
      max_age              int4  (최대 연령)
      income_condition_raw text  (소득 조건 원문)
      max_income           int4  (월 소득 상한, 원 단위 정수)
      apply_start_date     date  (신청 시작일)
      apply_end_date       date  (신청 마감일 = 우리가 추출하는 deadline)
      apply_period_raw     text  (신청기간 원문)
      region               text  ('전국' | '서울' | '인천')
      source_url           text  (원문 링크)

    Policy_Document
      id           (PK, DB에서 자동 생성되는 identity/uuid라고 가정 - insert 시 값 안 넣음)
      policy_id    text/varchar (FK -> Policy.policy_id)
      document_name text (구비서류/제출서류 항목명)
      is_required   bool

Policy_id에는 반드시 UNIQUE 제약(또는 PK)이 있어야 upsert(on_conflict)가 동작합니다.

------------------------------------------------------------------
환경변수 (.env 혹은 실행 전 export)
------------------------------------------------------------------
    SUPABASE_URL                Supabase 프로젝트 URL
    SUPABASE_SERVICE_ROLE_KEY   Supabase 서비스 롤 키 (서버 파이프라인이므로 service_role 권장)
    ONTONG_YOUTH_API_KEY        온통청년 오픈API 인증키 (마이페이지 > OPEN API 에서 발급)

------------------------------------------------------------------
실행 방법
------------------------------------------------------------------
    pip install -r requirements.txt
    python collect_policies.py --selftest   # 정규식 추출 로직만 빠르게 검증
    python collect_policies.py              # 실제 수집 + Supabase 적재

------------------------------------------------------------------
※ 지자체 스크래핑 관련 안내
------------------------------------------------------------------
서울/인천 포털은 화면 개편이 잦고 목록 카드의 실제 class 명을 라이브 DOM에서
직접 확인해야 정확히 잡을 수 있습니다. 아래 SeoulYouthPortalCollector /
IncheonYouthPortalCollector의 *_SELECTOR 상수에 TODO로 표시해두었으니,
브라우저 개발자도구(F12)로 실제 셀렉터를 확인해 한 줄만 바꿔주면 됩니다.
셀렉터가 안 맞아도 상세 페이지 전체 텍스트(body_text)를 정규식 추출기에 넘기므로
나이/소득/마감일/구비서류 추출 자체는 계속 동작합니다.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import requests  # type: ignore
from bs4 import BeautifulSoup

try:
    from supabase import create_client, Client
except ImportError:  # pragma: no cover
    create_client = None
    Client = None

try:
    from dotenv import load_dotenv

    load_dotenv()  # 같은 폴더의 .env 파일이 있으면 자동으로 읽어 환경변수로 등록
except ImportError:  # pragma: no cover
    pass


# ============================================================
# 0. 설정 (CONFIG)
# ============================================================

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")
ONTONG_API_KEY = os.environ.get("ONTONG_YOUTH_API_KEY")

REQUEST_TIMEOUT = 10
REQUEST_DELAY_SEC = 0.5  # 서버 부담을 줄이기 위한 요청 간 딜레이
USER_AGENT = "Mozilla/5.0 (compatible; CheongbajiCollector/1.0)"

# 실제 Supabase 테이블/컬럼명이 다르면 이 두 매핑만 고치면 됩니다.
POLICY_TABLE = "policies"
POLICY_DOCUMENT_TABLE = "policy_documents"

POLICY_COLUMNS = {
    "policy_id": "policy_id",
    "title": "title",
    "category": "category",
    "organization": "institution",
    "description": "summary",
    "support_content": "benefit_desc",
    "min_age": "min_age",
    "max_age": "max_age",
    "max_income": "max_income",
    "support_amount": "max_benefit_amount",
    "target_employment": "target_employment",
    "target_education": "target_education",
    "apply_end_date": "closing_date",
    "region": "target_region",
    "source_url": "apply_url",
}

POLICY_DOCUMENT_COLUMNS = {
    "policy_id": "policy_id",
    "document_name": "document_name",
    "is_required": "is_essential",
}

# 2026년 기준 중위소득 (보건복지부 고시, 월 단위 원). 정책 문구에 가구원수가
# 명시되지 않은 경우 청년 1인가구를 기본값으로 사용해 "중위소득 N%"를 원 단위로 환산합니다.
MEDIAN_INCOME_2026 = {
    1: 2_564_238,
    2: 4_199_292,
    3: 5_359_036,
    4: 6_494_738,
    5: 7_556_719,
    6: 8_555_952,
}
DEFAULT_HOUSEHOLD_SIZE = 1

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cheongbaji-collector")


# ============================================================
# 1. 데이터 모델
# ============================================================

@dataclass
class PolicyRecord:
    policy_id: str
    source: str
    title: str
    source_url: str = ""
    category: Optional[str] = None
    organization: Optional[str] = None
    description: Optional[str] = None
    support_content: Optional[str] = None
    min_age: Optional[int] = None
    max_age: Optional[int] = None
    income_condition_raw: Optional[str] = None
    max_income: Optional[int] = None
    support_amount: Optional[int] = None  # 지원금액(원 단위)
    target_employment: Optional[str] = None  # '전체' | '재직자' | '미취업자' | '자영업자'
    target_education: Optional[str] = None  # '전체' | '대학생' | '고졸이하'
    apply_start_date: Optional[str] = None  # 'YYYY-MM-DD'
    apply_end_date: Optional[str] = None    # 'YYYY-MM-DD' (마감일)
    apply_period_raw: Optional[str] = None
    region: Optional[str] = None
    documents: list = field(default_factory=list)  # List[str]


def _safe_int(value) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


# ============================================================
# 2. 정규식 기반 필드 추출 유틸  (요구사항 2, 3)
# ============================================================

_SEP = r"(?:[~\-–—∼～]|부터)"

_AGE_RANGE_RE = re.compile(rf"만?\s*(\d{{1,2}})\s*세\s*{_SEP}\s*만?\s*(\d{{1,2}})\s*세?")
_AGE_MAX_ONLY_RE = re.compile(r"만?\s*(\d{1,2})\s*세\s*(?:이하|미만|까지)")
_AGE_MIN_ONLY_RE = re.compile(r"만?\s*(\d{1,2})\s*세\s*(?:이상|부터)")


def extract_age_range(text: str) -> tuple[Optional[int], Optional[int]]:
    """'만 19세 ~ 34세 이하' -> (19, 34) 형태로 나이 범위를 정수로 추출.

    범위 표현이 없으면 '~세 이하'(상한만) / '~세 이상'(하한만)을 각각 시도한다.
    """
    if not text:
        return None, None

    m = _AGE_RANGE_RE.search(text)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        return min(a, b), max(a, b)

    min_age = None
    max_age = None
    m_max = _AGE_MAX_ONLY_RE.search(text)
    if m_max:
        max_age = int(m_max.group(1))
    m_min = _AGE_MIN_ONLY_RE.search(text)
    if m_min:
        min_age = int(m_min.group(1))
    return min_age, max_age


_INCOME_PERCENT_RE = re.compile(r"중위\s*소득\s*(\d{1,3})\s*%\s*(?:이하|미만)?")
_INCOME_MAN_WON_RE = re.compile(r"월\s*(?:평균)?\s*소득\s*([\d,]+(?:\.\d+)?)\s*만\s*원\s*(?:이하|미만)")
_INCOME_WON_RE = re.compile(r"월\s*(?:평균)?\s*소득\s*([\d,]{4,})\s*원\s*(?:이하|미만)")


def extract_max_income(
    text: str, household_size: int = DEFAULT_HOUSEHOLD_SIZE
) -> tuple[Optional[int], Optional[str]]:
    """소득 조건 문구에서 월 소득 상한(원 단위 정수)을 추출.

    - '중위소득 120% 이하'  -> 기준 중위소득(household_size인 가구) * 120% 로 환산
    - '월 250만원 이하'      -> 2,500,000
    - '월 2,500,000원 이하'  -> 2,500,000
    반환값: (max_income:int|None, 매칭된 원문 조각:str|None)
    """
    if not text:
        return None, None

    m = _INCOME_PERCENT_RE.search(text)
    if m:
        percent = int(m.group(1))
        base = MEDIAN_INCOME_2026.get(household_size, MEDIAN_INCOME_2026[DEFAULT_HOUSEHOLD_SIZE])
        value = round(base * percent / 100)
        return value, m.group(0).strip()

    m = _INCOME_MAN_WON_RE.search(text)
    if m:
        value = float(m.group(1).replace(",", "")) * 10_000
        return int(value), m.group(0).strip()

    m = _INCOME_WON_RE.search(text)
    if m:
        value = int(m.group(1).replace(",", ""))
        return value, m.group(0).strip()

    return None, None


_DATE_RE = re.compile(r"(20\d{2})\s*[.\-/년]\s*(\d{1,2})\s*[.\-/월]\s*(\d{1,2})\s*일?")


def extract_deadline(text: str) -> Optional[str]:
    """본문에서 날짜를 모두 찾아 YYYY-MM-DD로 정규화하고, 그중 가장 마지막(=보통
    '시작일 ~ 마감일'에서 마감일) 날짜를 반환한다. 날짜가 없으면 None
    ('상시모집', '예산 소진 시까지' 등).
    """
    if not text:
        return None

    dates = []
    for y, mo, d in _DATE_RE.findall(text):
        try:
            dates.append(date(int(y), int(mo), int(d)))
        except ValueError:
            continue
    if not dates:
        return None
    return max(dates).isoformat()


_DOC_LABEL_RE = r"(?:추진|구비|제출|첨부|준비|필요|신청|증빙)\s*서류"
# 라벨 뒤에 (a) 콜론이 바로 오거나, (b) 콜론 없이 바로 줄바꿈/불릿/번호가
# 오는 경우만 '서류 목록 섹션의 시작'으로 인정한다([ \t]*로 공백/탭만
# 건너뛰고 줄바꿈은 건너뛰지 않음에 주의 - 그래야 '라벨\n- 항목' 형태도
# 놓치지 않는다). 그 외의 경우(라벨 뒤에 공백만 두고 다른 낱말이 바로
# 오는 경우, 예: '제출서류 유의사항 안내')는 라벨이 서류 목록 헤더가
# 아니라 다른 문장(안내문 등) 속에 우연히 섞여 있는 것으로 보고 건너뛴다.
_DOC_SECTION_RE = re.compile(
    rf"{_DOC_LABEL_RE}[ \t]*(?:[:：][ \t]*|(?=\n|[-·•‣]|[①-⑩]|\d{{1,2}}\s*[.)]))(.+?)"
    r"(?=(?:\n\s*\n)|신청\s*방법|문의\s*처|접수\s*방법|접수\s*기간|유의\s*사항|"
    r"담당\s*부서|기타\s*안내|추진\s*절차|결과\s*통보|※|$)",
    re.S,
)
# 가)/나)/다)/라)처럼 공문서에 흔한 한글 순번 불릿도 번호/불릿 목록으로
# 인식한다 (기존에는 ①-⑩/숫자/대시만 인식해서, 이 형식의 서류 안내문이
# 통째로 하나의 항목으로 뭉쳐서 저장되는 문제가 있었음). "가" 같은 흔한
# 글자가 아무 단어에나 오인되지 않도록, 반드시 공백 뒤 + 바로 다음에
# '.'/')'가 와야만(= 그 글자 하나가 통째로 순번 라벨일 때만) 인정한다.
_DOC_ITEM_SPLIT_RE = re.compile(
    r"(?:^|\s)(?:[①-⑩]|\d{1,2}\s*[.)]|[-·•‣]|[가나다라마바사아자]\s*[.)])\s*"
)
_DOC_REQUIRED_TAG_SPLIT_RE = re.compile(r"(?<=\((?:필수|선택)\))\s*(?=\S)")


def _split_top_level(value: str, seps: str) -> list[str]:
    """괄호(/중괄호/대괄호) 안에 있지 않은(= 깊이 0인) 위치의 구분자에서만
    쪼갠다. 예: '미과세증명서(과세년도:2024~2026년, 자치단체:전국)'처럼
    서류명 조건을 설명하는 괄호 안의 쉼표까지 서류 구분자로 잘못 쪼개버리는
    것을 막기 위함 - 그 쉼표는 한 서류명 안의 세부조건 나열일 뿐 다른
    서류와의 구분자가 아니다."""
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    for ch in value:
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth = max(0, depth - 1)
        if depth == 0 and ch in seps:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    parts.append("".join(buf))
    return parts


def _split_document_items(value: str) -> list[str]:
    """이미 '~서류' 라벨에서 값만 떼어낸 문자열(라벨 자체는 포함되어 있지
    않음)을 항목 리스트로 분리한다. extract_required_documents()는 라벨
    글자를 먼저 찾아 그 뒤 섹션을 자르는 방식이라 라벨이 없는 값 자체에는
    쓸 수 없어서(예: 온통청년 API의 'sbmsnDcmntCn' 필드는 이미 서류 목록만
    떼어져 있고 '제출서류' 같은 라벨 글자가 없음 - 이걸 extract_required_
    documents()에 그대로 넣으면 라벨을 못 찾아서 빈 리스트가 되어버렸다),
    번호/불릿(①② , 1. 2) , ·‣) 우선 분리 후 안 나뉘면 쉼표/줄바꿈 기준으로
    분리하고, 그래도 여러 서류명이 한 항목에 붙어있으면(불릿 없이
    '여권사본(필수) 통장사본(선택)'처럼 나열된 경우) '(필수)'/'(선택)' 표시
    뒤에서, 그래도 안 나뉘면 괄호 밖(최상위) 쉼표 기준으로 한 번 더 쪼갠다
    (예: '대학생 : 온라인 신청서, 주민등록등본(...), 교육비 납입증명서'처럼
    대상군 라벨 뒤에 쉼표로만 나열된 경우)."""
    value = (value or "").strip()
    if not value:
        return []

    def _clean(parts: list[str]) -> list[str]:
        cleaned = [re.sub(r"\s+", " ", p).strip(" .,") for p in parts]
        return [p for p in cleaned if p and len(p) > 1]

    def _refine_one(it: str) -> list[str]:
        tag_parts = _clean(_DOC_REQUIRED_TAG_SPLIT_RE.split(it))
        if len(tag_parts) > 1:
            result: list[str] = []
            for p in tag_parts:
                result.extend(_refine_one(p))
            return result
        # 괄호 밖 쉼표가 하나라도 있으면(=괄호 안 세부조건이 아니라 서류명을
        # 나열하는 쉼표일 가능성이 높으면) 그 기준으로도 나눠본다.
        top_level = _clean(_split_top_level(it, ","))
        if len(top_level) > 1:
            return top_level
        return [it]

    items = _clean(_DOC_ITEM_SPLIT_RE.split(value))

    if len(items) <= 1:
        # 번호/불릿이 없으면 '(필수)'/'(선택)' 표시 뒤에서 먼저 재시도한다.
        # 쉼표로 먼저 쪼개면 서류명 괄호 안에 쉼표가 들어있는 경우(예:
        # '미과세증명서(과세년도:2024~2026년, 자치단체:전국)')에 그 안쪽
        # 까지 잘못 쪼개져버리는 문제가 있어서, 쉼표 분리는 (괄호 밖만 보는
        # 방식으로) 최후의 수단으로만 쓴다.
        tag_items = _clean(_DOC_REQUIRED_TAG_SPLIT_RE.split(value))
        if len(tag_items) > 1:
            items = tag_items
        else:
            return _clean(_split_top_level(value, ",\n·"))
    else:
        items_after_tag = []
        for it in items:
            parts = _clean(_DOC_REQUIRED_TAG_SPLIT_RE.split(it))
            items_after_tag.extend(parts if len(parts) > 1 else [it])
        items = items_after_tag

    # 번호/불릿/태그로 이미 나뉜 항목이라도, 그 안에 괄호 밖 쉼표로 여러
    # 서류명이 나열돼 있을 수 있어(예: 대상군 라벨 뒤 쉼표 나열) 한 번 더
    # 확인한다.
    refined: list[str] = []
    for it in items:
        refined.extend(_refine_one(it))
    return refined


def extract_required_documents(text: str) -> list[str]:
    """본문에서 '~서류' 라벨(구비서류/제출서류/첨부서류/추진서류/신청서류/
    준비서류/필요서류/증빙서류 등) 섹션을 찾아 항목 리스트로 분리한다.

    라벨 뒤에 콜론이 바로 오거나, 콜론 없이 바로 줄바꿈/불릿/번호가 오는
    경우만 '서류 목록 섹션의 시작'으로 인정한다 (예전에는 이 조건이 전혀
    없어서, 본문 중간에 전혀 무관한 문장 안에 라벨 글자가 우연히 들어있는
    경우까지 섹션 시작으로 오인해서 그 뒤의 전혀 무관한 텍스트를 통째로
    '서류 목록'으로 잘못 캡처하는 문제가 있었다 - 예: '※ 제출서류
    유의사항 안내 : 모든 서류는 PDF로 제출'은 서류 목록이 아니라 서류
    제출 방법에 대한 안내문인데, 라벨 글자 '제출서류' 뒤에 공백만 두고
    다른 낱말('유의사항')이 바로 이어진다는 걸 구분하지 못해서 잘못
    매칭됐었다).
    """
    if not text:
        return []

    m = _DOC_SECTION_RE.search(text)
    if not m:
        return []

    section = re.sub(r"[\r\n]+", " ", m.group(1)).strip()
    return _split_document_items(section)


# ------------------------------------------------------------------
# 인천청년포털 전용 보조 추출기: <title> 태그 기반 제목 정리, 카테고리 추론,
# 본문 영역(네비게이션 제외) 추출, 지원금액 추출
# ------------------------------------------------------------------

_CONTENT_SECTION_RE = re.compile(
    r"사업개요\s*(.+?)(?=문\s*의\s*처|담당\s*부서|\[목록\]|$)",
    re.S,
)

_SUPPORT_AMOUNT_RE = re.compile(
    r"(?:최대|지원\s*금액|지급액)\s*[^0-9]{0,10}?([\d,]+(?:\.\d+)?)\s*만\s*원"
)
_ANY_MAN_WON_RE = re.compile(r"([\d,]+(?:\.\d+)?)\s*만\s*원")

_CATEGORY_KEYWORDS = [
    ("주거", ["주택", "전세", "월세", "임차", "보증금", "입주", "주거", "이사"]),
    ("일자리", ["취업", "채용", "구직", "일자리", "창업", "인턴", "고용"]),
    ("교육", ["교육", "학비", "장학금", "수강", "자격증", "학자금", "강좌"]),
    ("금융", ["대출", "이자", "적금", "저축", "보험", "환급", "금융"]),
    ("문화", ["문화", "공연", "전시", "여행", "축제", "체육", "예술"]),
]


def extract_title_from_title_tag(soup: BeautifulSoup, fallback: str) -> str:
    """<title> 태그(예: '인천청년정책-실제제목 | 인천청년포털>...')에서
    사이트명/경로를 떼어내고 실제 정책명만 뽑아낸다."""
    if not soup.title or not soup.title.string:
        return fallback
    raw = soup.title.string.split("|")[0].strip()
    if raw.startswith("인천청년정책-"):
        raw = raw[len("인천청년정책-"):].strip()
    return raw or fallback


def infer_category(text: str) -> Optional[str]:
    """제목+본문 텍스트에 포함된 키워드로 정책 카테고리를 추론한다.
    일치하는 키워드가 없으면 None (호출부에서 '기타'로 대체)."""
    if not text:
        return None
    for category, keywords in _CATEGORY_KEYWORDS:
        if any(kw in text for kw in keywords):
            return category
    return None


def extract_main_content(text: str, max_len: int = 1500) -> str:
    """전체 페이지 텍스트에서 '사업개요' ~ '문의처'/'담당부서'/'[목록]' 사이,
    즉 메뉴/버튼을 뺀 실제 정책 설명 부분만 뽑아낸다. 못 찾으면 앞부분을 그대로 쓴다."""
    if not text:
        return ""
    m = _CONTENT_SECTION_RE.search(text)
    content = m.group(1) if m else text
    content = re.sub(r"\s+", " ", content).strip()
    return content[:max_len]


def extract_support_amount(text: str) -> Optional[int]:
    """'연간 최대 300만원' 같은 문구에서 지원금액(원 단위)을 추출한다."""
    if not text:
        return None
    m = _SUPPORT_AMOUNT_RE.search(text)
    if not m:
        m = _ANY_MAN_WON_RE.search(text)
    if not m:
        return None
    try:
        return int(float(m.group(1).replace(",", "")) * 10_000)
    except ValueError:
        return None


_EMPLOYMENT_KEYWORDS = [
    ("미취업자", ["미취업자", "구직자", "실업자", "취업준비생"]),
    ("재직자", ["재직자", "근로자", "직장인", "취업자"]),
    ("자영업자", ["자영업자", "소상공인"]),
]

_EDUCATION_KEYWORDS = [
    ("대학생", ["대학생", "대학원생", "재학생"]),
    ("고졸이하", ["고등학교 졸업", "고졸"]),
]


def infer_employment_target(text: str) -> str:
    """본문에서 특정 고용상태 제한 키워드를 찾으면 그 값을, 못 찾으면
    '전체'(= 제한없음, 누구나 지원 가능)를 반환한다. target_region의 '전국'과
    같은 방식으로, null 대신 명시적인 문자열을 넣기 위한 함수."""
    if text:
        for label, keywords in _EMPLOYMENT_KEYWORDS:
            if any(kw in text for kw in keywords):
                return label
    return "전체"


def infer_education_target(text: str) -> str:
    """본문에서 특정 학력/재학 조건 키워드를 찾으면 그 값을, 못 찾으면
    '전체'(= 제한없음)를 반환한다."""
    if text:
        for label, keywords in _EDUCATION_KEYWORDS:
            if any(kw in text for kw in keywords):
                return label
    return "전체"


# 수도권(서울/인천/경기) 판별용 키워드. '경기'는 '경기 활성화'처럼 지역이 아닌
# 일반 단어로도 흔히 쓰여서, 반드시 '경기도'까지 붙은 경우만 인정한다.
_CAPITAL_REGION_KEYWORDS = [
    ("서울", ["서울특별시", "서울시", "서울"]),
    ("인천", ["인천광역시", "인천시", "인천"]),
    ("경기", ["경기도"]),
]

# 수도권이 아닌 지역(주관기관명/본문에 명시된 경우) - 있으면 그 지역으로 확정해서
# "전국"으로 뭉개지 않고 정확히 걸러낼 수 있게 한다.
_OTHER_REGION_KEYWORDS = [
    # 부산/대구/대전/울산/세종/강원은 "~시"/"~도" 풀네임 없이 축약형만 쓰이는
    # 경우(예: "부산진구 청년 행정인턴", "강원 고성군 청년 취업수당")가 많아서
    # 축약형도 반드시 포함한다. ('부산'은 '부산진구' 등 모든 부산 산하 구/군
    # 이름에 접두어로 들어있어서 이 한 단어로 그 구/군까지 전부 잡힌다.)
    # '광주'만 예외: 경기도 광주시와 혼동될 수 있어 축약형을 넣지 않고
    # '광주광역시'/'광주시' 풀네임이 있을 때만 전라도 광주로 인정한다.
    ("부산", ["부산광역시", "부산시", "부산"]),
    ("대구", ["대구광역시", "대구시", "대구"]),
    ("광주", ["광주광역시", "광주시"]),
    ("대전", ["대전광역시", "대전시", "대전"]),
    ("울산", ["울산광역시", "울산시", "울산"]),
    ("세종", ["세종특별자치시", "세종시", "세종"]),
    ("강원", ["강원도", "강원특별자치도", "강원"]),
    ("충북", ["충청북도", "충북"]),
    ("충남", ["충청남도", "충남"]),
    ("전북", ["전라북도", "전북특별자치도", "전북"]),
    ("전남", ["전라남도", "전남"]),
    ("경북", ["경상북도", "경북"]),
    ("경남", ["경상남도", "경남"]),
    ("제주", ["제주특별자치도", "제주"]),
]


_ALL_REGION_KEYWORDS = _CAPITAL_REGION_KEYWORDS + _OTHER_REGION_KEYWORDS

# 중앙행정기관(부/처/청/위원회) 이름. 이런 기관이 주관하는 사업은 본문에
# "대구광역시, 대전광역시, 세종... 10만원 지원"처럼 지원금 차등 지급 지역이나
# "인하대(인천), 경성대(부산)..."처럼 참여 기관을 여러 지역에 걸쳐 나열하는
# 경우가 많다. 그런 나열 중 본문에서 가장 먼저 등장한 지역 하나만 소관 지역으로
# 오인하면 안 되므로, 주관기관이 중앙부처로 확인되면 본문 지역명 검사 없이
# 곧바로 '전국'으로 처리한다.
_CENTRAL_MINISTRY_NAMES = {
    "고용노동부", "교육부", "보건복지부", "여성가족부", "문화체육관광부",
    "국토교통부", "중소벤처기업부", "기획재정부", "산업통상자원부",
    "농림축산식품부", "과학기술정보통신부", "행정안전부", "국가보훈부",
    "통일부", "외교부", "국방부", "법무부", "환경부", "해양수산부",
    "식품의약품안전처", "병무청", "특허청", "통계청", "관세청", "국세청",
    "산림청", "기상청", "경찰청", "소방청", "금융위원회", "공정거래위원회",
    "국민권익위원회", "방송통신위원회", "개인정보보호위원회",
}


def infer_region_from_ontong(organization: Optional[str], text: str) -> str:
    """온통청년 API 응답에는 명시적인 지역 코드 필드가 없어서, 주관기관명과
    본문(제목+설명 등) 텍스트에 특정 시/도 이름이 나오는지로 지역을 추정한다.

    주관기관명을 본문 앞에 붙여 하나의 문자열로 합친 뒤, 그 안에서 "가장 먼저
    (왼쪽에서부터) 등장하는" 지역 키워드를 채택한다. 수도권 키워드를 항상
    먼저 검사하던 이전 방식은, 예를 들어 '충남학사관 운영 - 충남출신 대학생
    대상 ... 충청남도서울학사관(서울특별시 ...) 운영'처럼 실제 소관 지역(충남)이
    본문 앞쪽에 나오고 수도권 지명은 시설 주소 등으로 뒤쪽에만 잠깐 등장하는
    경우를, 뒤쪽의 수도권 지명 때문에 '서울'로 잘못 분류하는 문제가 있었다.
    정책 제목/소개는 관례적으로 소관 지역을 맨 앞에 명시하는 경우가 많아서,
    "가장 먼저 등장하는 지역명"이 실제 소관 지역일 가능성이 가장 높다.
    아무 지역명도 안 나오면 특정 지자체가 아닌 중앙부처 등이 운영하는 진짜
    전국 단위 정책으로 보고 '전국'을 반환한다.
    """
    if organization and organization.strip() in _CENTRAL_MINISTRY_NAMES:
        return "전국"

    combined = f"{organization or ''} {text or ''}"
    if not combined.strip():
        return "전국"

    best_pos: Optional[int] = None
    best_label: Optional[str] = None
    for label, keywords in _ALL_REGION_KEYWORDS:
        for kw in keywords:
            pos = combined.find(kw)
            if pos == -1:
                continue
            if best_pos is None or pos < best_pos:
                best_pos = pos
                best_label = label
    return best_label or "전국"


# ============================================================
# 3. 수집기 (requests + BeautifulSoup)  (요구사항 1)
# ============================================================

_session = requests.Session()
_session.headers.update({"User-Agent": USER_AGENT})


def http_get(url: str, params: dict | None = None, max_retries: int = 3) -> requests.Response:
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = _session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
                resp.encoding = resp.apparent_encoding or "utf-8"
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning("요청 실패(%d/%d) %s: %s", attempt, max_retries, url, exc)
            time.sleep(1.5 * attempt)
    raise last_exc  # type: ignore[misc]


class BaseCollector:
    source_name = "base"

    def fetch(self) -> list[PolicyRecord]:
        raise NotImplementedError


class OntongYouthCollector(BaseCollector):
    """온통청년 오픈API 수집기.

    엔드포인트/파라미터명은 https://www.youthcenter.go.kr/cmnFooter/openapiIntro/oaiDoc
    에 안내된 값을 기준으로 하되, 응답 필드의 정확한 코드값은 API 승인 후 내려받는
    'API코드정보.xlsx'로 반드시 재확인하세요. 아래 parse_item()은 흔히 쓰이는
    필드명을 후보로 두고 여러 이름을 순서대로 시도하도록(get()) 방어적으로 작성했습니다.
    """

    source_name = "ontong_youth"
    # 2026-07 신규 API 규격으로 엔드포인트/파라미터명이 변경됨.
    # (구) https://www.youthcenter.go.kr/opi/youthPlcyList.do?openApiVlak=&display=&pageIndex=
    # (신) https://www.youthcenter.go.kr/go/ythip/getPlcy?apiKeyNm=&pageSize=&pageNum=
    LIST_URL = "https://www.youthcenter.go.kr/go/ythip/getPlcy"

    def __init__(self, api_key: str | None = None, page_size: int = 100):
        self.api_key = api_key or ONTONG_API_KEY
        self.page_size = page_size

    def fetch_page(self, page_index: int) -> tuple[list[dict], int]:
        params = {
            "apiKeyNm": self.api_key,
            "pageSize": self.page_size,
            "pageNum": page_index,
            "rtnType": "json",
        }
        resp = http_get(self.LIST_URL, params=params)
        try:
            data = resp.json()
        except ValueError:
            logger.warning("온통청년 API 응답이 JSON이 아닙니다(XML일 수 있음) - 이 페이지는 건너뜁니다.")
            return [], 0

        result = data.get("result", data) if isinstance(data, dict) else {}
        items = (
            result.get("youthPolicyList")
            or result.get("youthPolicyDtlList")
            or result.get("list")
            or []
        )
        total = (
            (result.get("pagging") or {}).get("totCount")
            or result.get("totalCount")
            or len(items)
        )
        return items, int(total or 0)

    @staticmethod
    def _pick(item: dict, *keys):
        for k in keys:
            v = item.get(k)
            if v not in (None, ""):
                return v
        return None

    def parse_item(self, item: dict) -> PolicyRecord:
        pick = lambda *keys: self._pick(item, *keys)  # noqa: E731

        title = pick("plcyNm", "polyBizSjnm", "title") or "(제목없음)"
        raw_id = pick("plcyNo", "bizId", "id") or hashlib.md5(str(title).encode()).hexdigest()[:10]
        description = pick("plcyExplnCn", "polyItcnCn", "description") or ""
        support_content = pick("plcySprtCn", "sportContent") or ""
        organization = pick("sprvsnInstCdNm", "operInstCdNm", "rgtrInstCdNm")
        # 온통청년 API 자체의 대분류명(lclsfNm 등)은 "복지문화", "참여권리"처럼
        # 우리 서비스의 5개 관심분야(일자리/주거/금융/교육/문화)와 글자 그대로
        # 일치하지 않는 경우가 있어, 그대로 쓰면 해당 정책이 관심분야 필터·
        # 적합도 매칭에서 통째로 빠진다. 제목+본문 키워드 추론을 우선 적용하고,
        # 키워드로 못 찾을 때만 API 원본 대분류명을 그대로 둔다(최종적으로 못
        # 찾으면 DB 저장/프론트엔드 양쪽에서 '기타'로 처리됨).
        raw_category = pick("lclsfNm", "mclsfNm", "plcyKywdNm")
        category = infer_category(
            f"{title} {description} {support_content}"
        ) or raw_category
        apply_period_raw = pick("aplyYmd", "rqutPrdCn") or ""
        income_field_raw = pick("earnEtcCn")

        combined_text = " ".join(
            str(x)
            for x in [
                description,
                support_content,
                apply_period_raw,
                income_field_raw,
                pick("addAplyQlfcCndCn"),
            ]
            if x
        )

        min_age = _safe_int(item.get("sprtTrgtMinAge"))
        max_age = _safe_int(item.get("sprtTrgtMaxAge"))
        if min_age is None or max_age is None:
            re_min, re_max = extract_age_range(combined_text)
            min_age = min_age if min_age is not None else re_min
            max_age = max_age if max_age is not None else re_max

        max_income, income_condition_raw = extract_max_income(combined_text)
        if not income_condition_raw:
            income_condition_raw = income_field_raw

        deadline = None
        end_ymd = pick("aplyEndYmd")
        if end_ymd and re.match(r"^\d{8}$", str(end_ymd)):
            s = str(end_ymd)
            deadline = f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        if not deadline:
            deadline = extract_deadline(apply_period_raw) or extract_deadline(combined_text)

        start_date = None
        start_ymd = pick("aplyBgngYmd")
        if start_ymd and re.match(r"^\d{8}$", str(start_ymd)):
            s = str(start_ymd)
            start_date = f"{s[:4]}-{s[4:6]}-{s[6:8]}"

        # 온통청년 API의 '제출서류' 전용 필드(sbmsnDcmntCn)는 이미 서류
        # 목록만 떼어져 있어서(라벨 글자 자체가 없음) extract_required_
        # documents()처럼 라벨을 먼저 찾는 방식을 쓰면 라벨이 없다는 이유로
        # 못 찾고 빈 리스트가 되어버린다(실제로 전체 수집 정책 중 상당수가
        # 이 문제로 서류 목록이 비어있었다). 이 필드가 있으면 라벨 탐색 없이
        # 바로 항목만 분리하고, 없을 때만 본문 전체(combined_text)에서 라벨
        # 기준으로 찾는다.
        raw_doc_field = pick("sbmsnDcmntCn")
        documents = (
            _split_document_items(raw_doc_field)
            if raw_doc_field
            else extract_required_documents(combined_text)
        )

        return PolicyRecord(
            policy_id=f"ontong-{raw_id}",
            source=self.source_name,
            title=title,
            # aplyUrlAddr(실제 신청 URL)을 refUrlAddr1(참고/설명 사이트)보다
            # 우선한다. 예전에는 반대 순서라서, 실제로는 따로 신청 페이지가
            # 있는 정책도 전부 '참고용 설명 사이트'로만 연결되고 있었다.
            source_url=pick("aplyUrlAddr", "refUrlAddr1") or "https://www.youthcenter.go.kr",
            category=category,
            organization=organization,
            description=description,
            support_content=support_content,
            min_age=min_age,
            max_age=max_age,
            income_condition_raw=income_condition_raw,
            max_income=max_income,
            apply_start_date=start_date,
            apply_end_date=deadline,
            apply_period_raw=apply_period_raw,
            region=infer_region_from_ontong(organization, f"{title} {combined_text}"),
            documents=documents,
        )

    def fetch(self) -> list[PolicyRecord]:
        if not self.api_key:
            logger.warning("ONTONG_YOUTH_API_KEY가 없어 온통청년 수집을 건너뜁니다.")
            return []

        records: list[PolicyRecord] = []
        page = 1
        while True:
            items, total = self.fetch_page(page)
            if not items:
                break
            for item in items:
                try:
                    records.append(self.parse_item(item))
                except Exception:
                    logger.exception("온통청년 항목 파싱 실패: %s", item.get("plcyNo"))
            logger.info("온통청년 %d페이지 수집 완료 (누적 %d건 / 전체 %s건)", page, len(records), total)
            if len(items) < self.page_size or (total and len(records) >= total):
                break
            page += 1
            time.sleep(REQUEST_DELAY_SEC)
        return records


class SeoulYouthPortalCollector(BaseCollector):
    """서울시 청년몽땅정보통(youth.seoul.go.kr) HTML 스크레이핑 수집기.

    목록: /infoData/plcyInfo/list.do?key=2309150002&pageIndex=N (GET, '서울시 정책' 탭)
    상세: /infoData/plcyInfo/view.do?key=2309150002&tabKind=001&plcyBizId=<ID> (GET)

    (2026-11 기준, 브라우저 개발자도구로 실제 구조 확인 후 셀렉터를 맞춤)
    목록의 정책 카드는 href가 아니라 onclick="goView('<ID>')" 자바스크립트 호출로
    상세 페이지로 이동하는 구조라서, href 대신 onclick에서 정규식으로 plcyBizId를
    뽑아 상세 URL을 직접 구성한다. (goView()가 내부적으로 폼을 GET으로
    view.do에 제출하는 것을 그대로 흉내낸 것 — 실제 브라우저에서 동일한 GET
    URL로 상세 페이지가 정상적으로 열리는 것을 확인함.)

    상세 페이지는 <th>라벨</th><td>값</td> 형태의 표로 구성되어 있어서, 전체
    본문을 정규식으로 긁는 대신 라벨 기준으로 바로 값을 읽어온다(연령/
    사업신청기간/제출서류/신청 사이트/정책 유형 등 전부 표 안에 있음). 단, 한
    <tr> 안에 라벨/값 쌍이 두 개 들어있는 행도 있어서(예: '사업운영기간'|''|
    '사업신청기간'|'20260301 ~ 20261130') tr 단위로 첫 th/td만 보면 안 되고,
    문서 전체의 모든 th를 순회하며 각자의 바로 다음 형제 td를 찾아야 한다.
    """

    source_name = "seoul_youth_portal"
    BASE_URL = "https://youth.seoul.go.kr"
    LIST_URL = BASE_URL + "/infoData/plcyInfo/list.do"
    VIEW_URL = BASE_URL + "/infoData/plcyInfo/view.do"
    LIST_KEY = "2309150002"  # 목록/상세 페이지 식별용 쿼리 파라미터(사이트 구조상 고정값)

    LIST_ITEM_SELECTOR = "ul.policy-list > li"
    TITLE_SELECTOR = "a.tit"

    _GOVIEW_ID_RE = re.compile(r"goView\('([^']+)'\)")
    _PLCY_BIZ_ID_IN_HREF_RE = re.compile(r"plcyBizId=(\w+)")
    _YMD_RANGE_RE = re.compile(r"(\d{8})\s*~\s*(\d{8})")

    def __init__(self, max_pages: int = 3):
        self.max_pages = max_pages

    def fetch_list_page(self, page_no: int) -> list[tuple[str, str]]:
        resp = http_get(self.LIST_URL, params={"key": self.LIST_KEY, "pageIndex": page_no})
        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select(self.LIST_ITEM_SELECTOR)
        links: list[tuple[str, str]] = []
        for it in items:
            a = it.select_one(self.TITLE_SELECTOR) or it.find("a")
            if not a:
                continue
            title = a.get_text(strip=True)

            plcy_biz_id = None
            m = self._GOVIEW_ID_RE.search(a.get("onclick") or "")
            if m:
                plcy_biz_id = m.group(1)
            else:
                # 혹시 사이트 개편으로 onclick 대신 href에 id가 직접 들어오는
                # 경우를 대비한 대비책.
                m2 = self._PLCY_BIZ_ID_IN_HREF_RE.search(a.get("href") or "")
                if m2:
                    plcy_biz_id = m2.group(1)

            if not plcy_biz_id:
                continue
            links.append((title, plcy_biz_id))
        return links

    @staticmethod
    def _parse_labeled_rows(soup: BeautifulSoup) -> dict[str, str]:
        """상세 페이지의 <th>라벨</th><td>값</td> 표를 {라벨: 값} 딕셔너리로 변환."""
        rows: dict[str, str] = {}
        for th in soup.select("table th"):
            td = th.find_next_sibling("td")
            if td is None:
                continue
            label = th.get_text(strip=True)
            value = td.get_text(" ", strip=True)
            if label:
                rows[label] = value
        return rows

    def fetch_detail(self, title: str, plcy_biz_id: str) -> PolicyRecord:
        resp = http_get(
            self.VIEW_URL,
            params={"key": self.LIST_KEY, "tabKind": "001", "plcyBizId": plcy_biz_id},
        )
        soup = BeautifulSoup(resp.text, "html.parser")
        rows = self._parse_labeled_rows(soup)
        body_text = soup.get_text("\n", strip=True)

        description = rows.get("정책 소개") or ""
        support_content = rows.get("지원 내용") or ""
        raw_category = rows.get("정책 유형")
        organization = rows.get("운영기관") or rows.get("주관 기관")

        age_text = rows.get("연령") or ""
        combined_text = " ".join(
            x
            for x in [
                title,
                description,
                support_content,
                age_text,
                rows.get("학력"),
                rows.get("취업상태"),
                rows.get("추가단서 사항"),
                rows.get("참여제한 대상"),
            ]
            if x
        )

        category = infer_category(f"{title} {description} {support_content}") or raw_category

        min_age, max_age = extract_age_range(age_text)
        if min_age is None and max_age is None:
            min_age, max_age = extract_age_range(combined_text)

        max_income, income_condition_raw = extract_max_income(combined_text)

        apply_period_raw = rows.get("사업신청기간") or rows.get("사업운영기간") or ""
        start_date = None
        deadline = None
        m = self._YMD_RANGE_RE.search(apply_period_raw)
        if m:
            s, e = m.group(1), m.group(2)
            start_date = f"{s[:4]}-{s[4:6]}-{s[6:8]}"
            deadline = f"{e[:4]}-{e[4:6]}-{e[6:8]}"
        if not deadline:
            deadline = extract_deadline(apply_period_raw) or extract_deadline(body_text)

        # 주의: '제출서류' 칸이 비어있을 때 body_text(페이지 전체 텍스트) regex로
        # 대체 추출을 시도하면 안 된다 - body_text에도 '제출서류'라는 라벨
        # 글자 자체는 항상 존재해서(표에 빈 칸으로라도 찍힘) extract_required_
        # documents()가 그 뒤의 '신청 사이트/기타/운영기관...' 같은 무관한 표
        # 내용을 통째로 한 항목으로 잘못 집어오는 문제가 있었다(실제 테스트로
        # 확인됨). 이미 라벨로 값만 분리된 상태이므로, 그 값 자체를 번호/불릿/
        # 쉼표 기준으로만 나눈다. 값이 비어있으면 빈 리스트(=이 정책은 제출서류
        # 정보가 없음 - 프론트엔드에 이미 있는 안내 문구로 처리됨).
        documents = _split_document_items(rows.get("제출서류") or "")

        support_amount = extract_support_amount(support_content) or extract_support_amount(body_text)
        target_employment = infer_employment_target(rows.get("취업상태") or combined_text)
        target_education = infer_education_target(rows.get("학력") or combined_text)

        apply_url = rows.get("신청 사이트") or rows.get("참고 사이트 Ⅰ")
        detail_url = resp.url
        source_url = apply_url if apply_url and apply_url.startswith("http") else detail_url

        policy_id = "seoul-" + hashlib.md5(plcy_biz_id.encode()).hexdigest()[:12]
        return PolicyRecord(
            policy_id=policy_id,
            source=self.source_name,
            title=title or "(제목없음)",
            source_url=source_url,
            category=category,
            organization=organization,
            description=description or body_text[:1000],
            support_content=support_content,
            min_age=min_age,
            max_age=max_age,
            income_condition_raw=income_condition_raw,
            max_income=max_income,
            support_amount=support_amount,
            target_employment=target_employment,
            target_education=target_education,
            apply_start_date=start_date,
            apply_end_date=deadline,
            apply_period_raw=apply_period_raw,
            region="서울",
            documents=documents,
        )

    def fetch(self) -> list[PolicyRecord]:
        records: list[PolicyRecord] = []
        for page in range(1, self.max_pages + 1):
            try:
                links = self.fetch_list_page(page)
            except Exception:
                logger.exception("서울 청년포털 목록 페이지 요청 실패: page=%d", page)
                break
            if not links:
                break
            for title, plcy_biz_id in links:
                try:
                    records.append(self.fetch_detail(title, plcy_biz_id))
                except Exception:
                    logger.exception("서울 청년포털 상세 파싱 실패: %s / %s", title, plcy_biz_id)
                time.sleep(REQUEST_DELAY_SEC)
            logger.info("서울 청년포털 %d페이지 수집 완료 (누적 %d건)", page, len(records))
        return records


class IncheonYouthPortalCollector(BaseCollector):
    """인천시 청년포털(youth.incheon.go.kr) HTML 스크레이핑 수집기.

    목록: /youthpolicy/youthPolicyInfoList.do?pgno=N
    상세: /youthpolicy/youthPolicyInfoDetail.do?poly_seq=N
    """

    source_name = "incheon_youth_portal"
    BASE_URL = "https://youth.incheon.go.kr"
    LIST_URL = BASE_URL + "/youthpolicy/youthPolicyInfoList.do"
    DETAIL_URL = BASE_URL + "/youthpolicy/youthPolicyInfoDetail.do"

    def __init__(self, max_pages: int = 3):
        self.max_pages = max_pages

    def fetch_list_page(self, page_no: int) -> list[str]:
        resp = http_get(self.LIST_URL, params={"pgno": page_no})
        soup = BeautifulSoup(resp.text, "html.parser")
        seqs: set[str] = set()

        for a in soup.find_all("a", href=True):
            m = re.search(r"poly_seq=(\d+)", a["href"])
            if m:
                seqs.add(m.group(1))

        # onclick="fnDetail('123')" 형태의 JS 클릭 이벤트도 함께 시도
        for tag in soup.find_all(onclick=True):
            m = re.search(r"poly_seq['\"]?\s*[:=]\s*['\"]?(\d+)", tag["onclick"])
            if m:
                seqs.add(m.group(1))

        return sorted(seqs, key=int)

    def fetch_detail(self, poly_seq: str) -> PolicyRecord:
        url = f"{self.DETAIL_URL}?poly_seq={poly_seq}"
        resp = http_get(self.DETAIL_URL, params={"poly_seq": poly_seq})
        soup = BeautifulSoup(resp.text, "html.parser")

        # 상세 페이지에는 '신청하기' 버튼(a.btn_join, 실제 신청 접수 폼으로
        # 연결됨)이 따로 있는데, 지금까지는 이걸 안 보고 그냥 지금 보고 있는
        # 상세 설명 페이지 자신의 URL(= url)을 신청 링크로 써버리고 있었다.
        # 버튼이 있으면 그 링크를 쓰고, 없으면(사이트 구조가 바뀐 경우 대비)
        # 기존처럼 상세 페이지 URL로 대체한다.
        apply_link_tag = soup.select_one("a.btn_join") or soup.find(
            "a", href=re.compile(r"AcptReg")
        )
        apply_href = apply_link_tag.get("href") if apply_link_tag else None
        if apply_href:
            apply_url = apply_href if apply_href.startswith("http") else self.BASE_URL + apply_href
        else:
            apply_url = url

        title = extract_title_from_title_tag(soup, fallback=f"인천청년정책-{poly_seq}")
        body_text = soup.get_text("\n", strip=True)
        main_content = extract_main_content(body_text)

        # 나이/소득/마감일/구비서류/지원금액은 전체 페이지 텍스트가 아니라
        # '사업개요~문의처' 구간(main_content)만 보고 뽑는다. 전체 텍스트에는
        # 사이트 공통 안내문구(예: "기준 중위소득 60% 이하 모의계산" 같은
        # 사이트 전반에 깔린 배너 문구)가 섞여 있어서, 이걸 그대로 쓰면 이
        # 정책과 무관한 값이 엉뚱하게 뽑히는 문제가 있었다.
        min_age, max_age = extract_age_range(main_content)
        max_income, income_raw = extract_max_income(main_content)
        deadline = extract_deadline(main_content)
        documents = extract_required_documents(main_content)
        support_amount = extract_support_amount(main_content)
        category = infer_category(title + " " + main_content)
        target_employment = infer_employment_target(main_content)
        target_education = infer_education_target(main_content)

        return PolicyRecord(
            policy_id=f"incheon-{poly_seq}",
            source=self.source_name,
            title=title,
            source_url=apply_url,
            region="인천",
            category=category,
            description=main_content,
            support_amount=support_amount,
            target_employment=target_employment,
            target_education=target_education,
            min_age=min_age,
            max_age=max_age,
            income_condition_raw=income_raw,
            max_income=max_income,
            apply_end_date=deadline,
            documents=documents,
        )

    def fetch(self) -> list[PolicyRecord]:
        records: list[PolicyRecord] = []
        for page in range(1, self.max_pages + 1):
            try:
                seqs = self.fetch_list_page(page)
            except Exception:
                logger.exception("인천 청년포털 목록 페이지 요청 실패: page=%d", page)
                break
            if not seqs:
                break
            for seq in seqs:
                try:
                    records.append(self.fetch_detail(seq))
                except Exception:
                    logger.exception("인천 청년포털 상세 파싱 실패: poly_seq=%s", seq)
                time.sleep(REQUEST_DELAY_SEC)
            logger.info("인천 청년포털 %d페이지 수집 완료 (누적 %d건)", page, len(records))
        return records


# ============================================================
# 4. Supabase 일괄 upsert  (요구사항 4)
# ============================================================

def get_supabase_client() -> "Client":
    if create_client is None:
        raise RuntimeError("supabase-py가 설치되어 있지 않습니다. `pip install supabase` 후 다시 실행하세요.")
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY 환경변수를 설정해주세요.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _map_columns(data: dict, column_map: dict) -> dict:
    return {column_map[k]: v for k, v in data.items() if k in column_map and v is not None}


def _cap(value, max_len: int = 95):
    """글자 수가 max_len을 넘으면 잘라낸다 (DB 컬럼 길이 제한 대응)."""
    if value is None:
        return None
    s = str(value)
    return s if len(s) <= max_len else s[:max_len]


def upsert_policies(client: "Client", records: list[PolicyRecord], batch_size: int = 200) -> int:
    rows = []
    for r in records:
        row = _map_columns(
            {
                "policy_id": r.policy_id,
                "source": r.source,
                "title": r.title,
                "category": r.category or "기타",
                "organization": r.organization or "미상",
                "description": r.description,
                "support_content": r.support_content or r.description,
                "min_age": r.min_age if r.min_age is not None else 19,
                "max_age": r.max_age if r.max_age is not None else 39,
                "income_condition_raw": r.income_condition_raw,
                "max_income": r.max_income,
                "support_amount": r.support_amount,
                "target_employment": r.target_employment,
                "target_education": r.target_education,
                "apply_start_date": r.apply_start_date,
                "apply_end_date": r.apply_end_date,
                "apply_period_raw": r.apply_period_raw,
                "region": r.region,
                "source_url": r.source_url,
            },
            POLICY_COLUMNS,
        )
        rows.append(row)

    pk_col = POLICY_COLUMNS["policy_id"]
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        client.table(POLICY_TABLE).upsert(chunk, on_conflict=pk_col).execute()
        logger.info("Policy upsert %d~%d / %d건 완료", i + 1, i + len(chunk), len(rows))
    return len(rows)


def upsert_policy_documents(client: "Client", records: list[PolicyRecord], batch_size: int = 200) -> int:
    fk_col = POLICY_DOCUMENT_COLUMNS["policy_id"]

    # ⚠️ 예전에는 "이번 회차에 서류가 1건 이상 뽑힌 policy_id"만 지우고 다시
    # 썼다. 그런데 추출 로직이 더 엄격해지면서(예: 라벨 뒤에 콜론/줄바꿈/불릿이
    # 바로 와야만 인정) 어떤 정책은 이번 회차에 0건으로 바뀔 수 있는데, 그
    # policy_id는 "서류가 있는 목록"에서 빠지니까 예전 회차 때 들어간
    # (옛날 버그로 뭉쳐지거나 중복된) 행이 영영 안 지워지고 DB에 계속
    # 남아있는 문제가 있었다. 그래서 이번 회차에 수집된 policy_id는
    # (서류가 0건이든 아니든) 전부 먼저 지우고 다시 쓴다 - 이번에 0건이면
    # 정말로 빈 상태로 남아야 맞다.
    policy_ids_this_run = list({r.policy_id for r in records})
    if policy_ids_this_run:
        client.table(POLICY_DOCUMENT_TABLE).delete().in_(fk_col, policy_ids_this_run).execute()

    rows = []
    for r in records:
        # 같은 정책 안에서 완전히 동일한 서류명이 여러 번 들어오는 경우(예:
        # 대상 구간마다 반복되는 공통 안내문 "기타 증빙자료 추가 제출(필요시)
        # (선택)")가 있어서, 체크리스트에 같은 항목이 여러 번 중복으로
        # 보이지 않도록 정책 단위로 중복을 제거한다 (순서는 유지).
        seen_doc_names: set[str] = set()
        for doc_name in r.documents:
            if doc_name in seen_doc_names:
                continue
            seen_doc_names.add(doc_name)
            row = _map_columns(
                {
                    "policy_id": r.policy_id,
                    "document_name": doc_name,
                    "is_required": True,
                },
                POLICY_DOCUMENT_COLUMNS,
            )
            rows.append(row)

    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        client.table(POLICY_DOCUMENT_TABLE).insert(chunk).execute()
        logger.info("Policy_Document insert %d~%d / %d건 완료", i + 1, i + len(chunk), len(rows))
    return len(rows)


def delete_policies_by_id(client: "Client", policy_ids: list[str], batch_size: int = 200) -> int:
    """주어진 policy_id들을 Policy_Document -> Policy 순으로 삭제한다.

    이번 실행에서 "수도권(전국/서울/인천/경기)이 아닌 지역"으로 새로 확인된
    정책들을 DB에서도 정리하기 위한 함수. upsert는 이번 배치에 없는 policy_id는
    건드리지 않기 때문에, 과거(예: 지역 추정 로직 적용 전)에 '전국'으로 잘못
    들어가 있던 지방 전용 정책이 계속 남아있을 수 있다 - 이 함수가 그걸 지운다.
    """
    if not policy_ids:
        return 0
    pk_col = POLICY_COLUMNS["policy_id"]
    fk_col = POLICY_DOCUMENT_COLUMNS["policy_id"]
    for i in range(0, len(policy_ids), batch_size):
        chunk = policy_ids[i : i + batch_size]
        client.table(POLICY_DOCUMENT_TABLE).delete().in_(fk_col, chunk).execute()
        client.table(POLICY_TABLE).delete().in_(pk_col, chunk).execute()
        logger.info("수도권 외 지역 정책 삭제 %d~%d / %d건 완료", i + 1, i + len(chunk), len(policy_ids))
    return len(policy_ids)


# ============================================================
# 5. 메인 파이프라인
# ============================================================

def run() -> None:
    collectors: list[BaseCollector] = [
        OntongYouthCollector(),
        SeoulYouthPortalCollector(max_pages=int(os.environ.get("SEOUL_MAX_PAGES", 3))),
        IncheonYouthPortalCollector(max_pages=int(os.environ.get("INCHEON_MAX_PAGES", 3))),
    ]

    all_records: list[PolicyRecord] = []
    for collector in collectors:
        logger.info("=== %s 수집 시작 ===", collector.source_name)
        try:
            records = collector.fetch()
        except Exception:
            logger.exception("%s 수집 중 오류 발생 - 건너뜁니다.", collector.source_name)
            continue
        logger.info("=== %s 수집 완료: %d건 ===", collector.source_name, len(records))
        all_records.extend(records)

    if not all_records:
        logger.warning("수집된 정책이 없습니다. 종료합니다.")
        return

    # 서비스 범위를 수도권(서울/인천/경기)으로 한정. '전국' 단위 정책까지 포함하면
    # 양이 너무 많아져서, 특정 지자체(수도권) 전용 정책만 남기고 '전국'을 포함한
    # 다른 모든 지역은 제외한다.
    ALLOWED_REGIONS = {"서울", "인천", "경기"}
    before_count = len(all_records)
    excluded_records = [r for r in all_records if r.region not in ALLOWED_REGIONS]
    all_records = [r for r in all_records if r.region in ALLOWED_REGIONS]
    logger.info(
        "수도권(서울/인천/경기) 외 지역 정책 %d건 제외, 남은 %d건",
        before_count - len(all_records),
        len(all_records),
    )

    # 같은 policy_id가 여러 페이지에서 중복으로 수집될 수 있으므로,
    # Supabase에 보내기 전에 policy_id 기준으로 중복을 한 번 제거한다(마지막 값만 유지).
    dedup: dict[str, PolicyRecord] = {}
    for r in all_records:
        dedup[r.policy_id] = r
    all_records = list(dedup.values())
    logger.info("중복 제거 후 최종 %d건", len(all_records))

    client = get_supabase_client()
    n_policies = upsert_policies(client, all_records)
    n_docs = upsert_policy_documents(client, all_records)
    logger.info("완료: Policy %d건, Policy_Document %d건 upsert", n_policies, n_docs)

    # upsert는 "이번 배치에 포함된 policy_id"만 건드리기 때문에, 과거 실행에서
    # (지역 추정 로직 적용 전) '전국'으로 잘못 저장된 뒤 이번에 수도권 외 지역으로
    # 새로 확인된 정책들은 DB에 그대로 남아있을 수 있다. 그 stale 데이터를 정리한다.
    excluded_ids = sorted({r.policy_id for r in excluded_records})
    if excluded_ids:
        n_deleted = delete_policies_by_id(client, excluded_ids)
        logger.info("수도권 외 지역으로 확인되어 DB에서 삭제한 정책: %d건", n_deleted)


# ============================================================
# 6. 정규식 추출기 자체 테스트 (--selftest)
# ============================================================

def _run_regex_selftests() -> None:
    print("[나이] '만 19세 ~ 34세 이하' ->", extract_age_range("지원대상: 만 19세 ~ 34세 이하 청년"))
    print("[나이] '만 39세 이하' ->", extract_age_range("만 39세 이하 무주택 청년"))
    print("[나이] '만 18세 이상' ->", extract_age_range("만 18세 이상 대학생"))

    income1 = extract_max_income("가구소득 기준 중위소득 120% 이하")
    print("[소득] '중위소득 120% 이하' -> max_income={}, 근거={}".format(*income1))

    income2 = extract_max_income("월 소득 250만원 이하인 자")
    print("[소득] '월 250만원 이하' -> max_income={}, 근거={}".format(*income2))

    income3 = extract_max_income("신청자 본인의 월 소득 2,500,000원 이하")
    print("[소득] '월 2,500,000원 이하' -> max_income={}, 근거={}".format(*income3))

    print(
        "[마감일] '신청기간 2026.09.01. ~ 2026.09.30.' ->",
        extract_deadline("신청기간 2026.09.01. ~ 2026.09.30. 까지 접수"),
    )
    print("[마감일] '상시모집' ->", extract_deadline("접수기간: 상시모집(예산 소진 시 마감)"))

    doc_text = (
        "■ 제출서류: 1. 신청서 1부 2. 주민등록등본 1부 3. 통장사본 1부\n\n"
        "신청방법: 온라인 접수"
    )
    print("[구비서류]", extract_required_documents(doc_text))

    doc_text2 = "구비서류\n- 재직증명서\n- 소득금액증명원\n- 가족관계증명서\n\n문의처: 02-000-0000"
    print("[구비서류]", extract_required_documents(doc_text2))

    print(
        "[제목정리] ->",
        extract_title_from_title_tag(
            BeautifulSoup(
                "<title>인천청년정책-아이플러스(i+) 집드림_1.0 이자지원 | 인천청년포털</title>",
                "html.parser",
            ),
            fallback="(제목없음)",
        ),
    )
    print("[카테고리추론] '전세 보증금 이자 지원' ->", infer_category("전세 보증금 이자 지원"))
    print("[지원금액] '연간 최대 300만원/가구' ->", extract_support_amount("연간 최대 300만원/가구 지원"))
    print("[고용상태] '미취업 청년 구직자 대상' ->", infer_employment_target("미취업 청년 구직자 대상"))
    print("[고용상태] (조건 없음) ->", infer_employment_target("만 19세 이상 인천시민 누구나"))
    print("[학력] '대학생 재학생 대상' ->", infer_education_target("대학생 재학생 대상"))

    assert extract_age_range("만 19세 ~ 34세 이하")[0] == 19
    assert extract_age_range("만 19세 ~ 34세 이하")[1] == 34
    assert extract_deadline("신청기간 2026.09.01. ~ 2026.09.30. 까지") == "2026-09-30"
    assert extract_max_income("월 소득 2,500,000원 이하")[0] == 2_500_000
    assert extract_required_documents(doc_text) == ["신청서 1부", "주민등록등본 1부", "통장사본 1부"]
    assert infer_category("전세 보증금 이자 지원") == "주거"
    assert extract_support_amount("연간 최대 300만원/가구 지원") == 3_000_000
    assert infer_employment_target("미취업 청년 구직자 대상") == "미취업자"
    assert infer_employment_target("만 19세 이상 인천시민 누구나") == "전체"
    assert infer_education_target("대학생 재학생 대상") == "대학생"
    print("\n모든 self-test 통과 ✅")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="청바지 청년정책 수집 파이프라인")
    parser.add_argument("--selftest", action="store_true", help="정규식 추출기만 빠르게 검증하고 종료")
    args = parser.parse_args()

    if args.selftest:
        _run_regex_selftests()
    else:
        run()

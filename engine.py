"""
engine.py - 공사명칭 검토 엔진 v3.0
"""

import os
import re
import bisect
from datetime import datetime
from difflib import SequenceMatcher

_LAST_MASTER_LOAD_ERROR = ""


# ═══════════════════════════════════════════════════════════
#  파일 텍스트 추출 (지연 import)
# ═══════════════════════════════════════════════════════════
SUPPORTED_EXTENSIONS = {'.xlsx', '.pdf', '.docx', '.hwp', '.csv'}


def extract_from_xlsx(filepath: str) -> list:
    from openpyxl import load_workbook
    texts = []
    wb = load_workbook(filepath, data_only=True)
    for ws in wb.worksheets:
        for row in ws.iter_rows(values_only=False):
            for cell in row:
                if cell.value is not None:
                    val = str(cell.value).strip()
                    if val:
                        loc = f"[{ws.title}] {cell.coordinate}"
                        texts.append((loc, val))
    wb.close()
    return texts


def extract_from_pdf(filepath: str) -> list:
    texts = []

    try:
        import fitz  # PyMuPDF

        doc = fitz.open(filepath)
        try:
            for page_num, page in enumerate(doc, 1):
                page_text = page.get_text()
                if not page_text:
                    continue
                for line_num, line in enumerate(page_text.split('\n'), 1):
                    line = line.strip()
                    if line:
                        texts.append((f"P{page_num} L{line_num}", line))
        finally:
            doc.close()
        return texts
    except ImportError:
        pass

    import pdfplumber
    with pdfplumber.open(filepath) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            page_text = page.extract_text()
            if not page_text:
                continue
            for line_num, line in enumerate(page_text.split('\n'), 1):
                line = line.strip()
                if line:
                    texts.append((f"P{page_num} L{line_num}", line))
    return texts


def extract_from_docx(filepath: str) -> list:
    from docx import Document
    texts = []
    doc = Document(filepath)
    for i, para in enumerate(doc.paragraphs, 1):
        t = para.text.strip()
        if t:
            texts.append((f"문단{i}", t))
    for ti, table in enumerate(doc.tables, 1):
        for ri, row in enumerate(table.rows, 1):
            for ci, cell in enumerate(row.cells, 1):
                t = cell.text.strip()
                if t:
                    texts.append((f"표{ti}({ri},{ci})", t))
    return texts


def extract_from_hwp(filepath: str) -> list:
    texts = []
    hwp = None
    try:
        import pyhwpx
        hwp = pyhwpx.Hwp(visible=False)
        hwp.open(filepath)
        raw = hwp.get_text()
        if isinstance(raw, str):
            lines = raw.split('\n')
        elif isinstance(raw, list):
            lines = [str(x) for x in raw]
        else:
            lines = [str(raw)]
        for i, line in enumerate(lines, 1):
            line = line.strip()
            if line:
                texts.append((f"HWP L{i}", line))
    except ImportError:
        raise ImportError("HWP: pyhwpx와 한컴오피스가 필요합니다.")
    except Exception as e:
        raise Exception(f"HWP 읽기 실패: {e}")
    finally:
        if hwp:
            try:
                hwp.quit()
            except Exception:
                pass
    return texts


def extract_from_csv(filepath: str) -> list:
    import csv

    texts = []
    for encoding in ['utf-8', 'cp949', 'euc-kr']:
        try:
            with open(filepath, 'r', encoding=encoding, newline='') as f:
                reader = csv.reader(f)
                for ri, row in enumerate(reader, 1):
                    for ci, val in enumerate(row, 1):
                        val = val.strip()
                        if val:
                            texts.append((f"R{ri} C{ci}", val))
            return texts
        except UnicodeDecodeError:
            continue

    raise ValueError(f"CSV 인코딩 인식 불가: {filepath}")


def extract_text_from_file(filepath: str, include_pdf_tables: bool = False) -> list:
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".pdf":
        return extract_from_pdf(filepath)

    dispatch = {
        ".xlsx": extract_from_xlsx,
        ".docx": extract_from_docx,
        ".hwp": extract_from_hwp,
        ".csv": extract_from_csv,
    }
    func = dispatch.get(ext)
    if func is None:
        raise ValueError(f"지원하지 않는 형식: {ext}")
    return func(filepath)


def fetch_master_from_server(url: str, timeout: int = 10) -> list:
    import csv
    import io
    import requests

    response = requests.get(url, timeout=timeout)
    response.raise_for_status()

    raw_text = _decode_master_csv_text(response.content)
    reader = csv.reader(io.StringIO(raw_text))
    raw_names = [row[0] for row in reader if row]
    names = _sanitize_master_names(raw_names)
    if len(names) < 10:
        raise ValueError("서버 데이터가 너무 적습니다")

    return names


# ═══════════════════════════════════════════════════════════
#  기본 마스터 명칭
# ═══════════════════════════════════════════════════════════
def _sanitize_master_names(raw_names: list) -> list:
    sanitized = []
    seen = set()
    for raw in raw_names:
        if raw is None:
            continue
        name = str(raw).strip().lstrip("\ufeff")
        if not name or name in seen:
            continue
        seen.add(name)
        sanitized.append(name)
    return sanitized


def _decode_master_csv_text(raw: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


DEFAULT_MASTER_NAMES = [
    "(민간)부산미음동물류",
    "(CM)평택고덕10A",
    "(종평)안심뉴타운A",
    "(적격)서해선103역사",
    "(민간)안성CDC물류",
    "(민간)제주한화우주센터",
    "(민간)쿠팡김천물류",
    "(민간)전주대자인병원",
    "(BOT)고삼호수 휴게소",
    "(기본)이천호국원확충",
    "(종심)운정역환승주차장",
    "(BTL)춘천기계공고",
    "(BTL)춘천기계공고(봄내중)",
    "(적격)고양주차장전기",
    "(종심)의왕월암1A",
    "(종심)경산대임1A",
    "(종심)인천계양5A",
    "(종심)성남복정1A",
    "(적격)강원철도청사",
    "(민간)광주오포D물류",
    "(민간)고려대인문관",
    "(BTL)경북대시설개선",
    "(BOT)목감휴게소신축",
    "(종심)남양주 왕숙 A-2BL",
    "(턴키)세종 5-1생활권 L5",
    "(기본)킨텍스전시장",
    "(BTL)논산관사",
    "(BTL)충남대 세종캠퍼스",
    "(BTL)공주대 세종캠퍼스",
    "(민참.분양)부천역곡대장A",
    "(민참.분양)광명시흥A",
    "(실시)부산지방합동청사",
    "(민간)KT대전인재개발원(전기)",
    "(민간)KT대전인재개발원(소방)",
    "(턴키)광교공공지산건립",
    "(시행도급)대전죽동오피스텔",
    "(시행도급)천안백석지산",
    "(시행도급)속초조양동생숙",
    "(자체.시공)인천검단1BL",
    "(자체.시공)인천검단3BL",
    "(시행도급)성남복정3BL",
    "(자체.시공)창원명곡1BL",
    "(자체.시공)시흥거모3BL",
    "(민임)이천중리4BL",
    "(시행도급)천안 오룡경기장",
    "(자체.시공)이천부필리A물류",
    "(자체.시공)서울역삼오피스텔",
    "(시행도급)성남복정홍보관",
    "(자체.시행)인천검단1BL",
    "(자체.시행)인천검단3BL",
    "(자체.시행)창원명곡1BL",
    "(자체.시행)서울역삼오피스텔",
    "(자체.시행)이천부필리A물류",
    "(자체.공모)오산운암뜰",
    "(종평)청주공공하수",
    "(종심)창녕밀양5",
    "(순수내역)포승-평택 2공구",
    "(턴키)이천문경철도8",
    "(턴키)삼성동탄철도2",
    "(턴키)안성구리10",
    "(턴키)안성구리11",
    "(종심)안성용인5",
    "(턴키)안성구리14",
    "(턴키)진접복선전철1",
    "(턴키)별내선복선전철3",
    "(적격)미금역외승강설비",
    "(적격)평택이화공공하수",
    "(적격)화천사내하수관로",
    "(적격)화천사내상수병설",
    "(종심)현대ENF천연가스",
    "(적격)서산씨지앤대산천연가스",
    "(종평)연금금성도로",
    "(종평)판교 하수처리용량 증설",
    "(적격)원산도공공하수",
    "(턴키)부산북항배후도로",
    "(실시)새만금전주8",
    "(적격)구리갈매송전관로",
    "(종평)매리양산도로",
    "(종심)포항안동2도로",
    "(종심)월곶판교전철5",
    "(종심)중리천리1도로",
    "(종심)대청댐광역상수도2",
    "(종평)울산미포산단",
    "(종평)송도기반시설",
    "(적격)장흥기산천재해예방",
    "(적격)송도배전간선",
    "(종심)새만금지구 3공구",
    "(종심)과천지식정보타운역 노반신설",
    "(종심)춘천-속초(6공구)",
    "(종심)강릉-제진(7공구)",
    "(실시)동면-진천(2공구)",
    "(턴키)용담댐 안전성 강화",
    "(실시)함양창녕3",
    "(종심)함양창녕8",
    "(턴키)삼성동탄철도4",
    "(턴키)동탄인덕원전철9",
    "(턴키)광교호매실전철2",
    "(턴키)대전철도중정비",
    "(CM)성남복정단지조성",
    "(CM)구리갈매단지조성",
    "(CM)시흥거모단지조성",
    "(CM)남양주왕숙조성2",
    "(턴키)안동댐안전성강화",
    "(기본)영동대로2도로",
    "(턴키)강북정수장증설",
    "(적격)성남복정전력구",
    "(해외)인도네시아도수관로",
    "(적격)화성능동조경",
    "(적격)파주운정조경",
    "(적격)경기 지방정원 조성",
    "(자체.시공)이천부필리도로",
    "(자체)화성우정 산단",
    "(해외)캄보디아지방도",
    "(통합)21년 창원부산",
    "(통합)23년 대구순환",
    "(통합)24년 상주영천(추가)",
    "(도로)22년 서울외곽",
    "(도로)22년 대구부산",
    "(도로)23년 신공항HW",
    "(도로)16년 광주원주",
    "(영업)23년 평택시흥",
    "(영업)22년 서울문산",
    "(ITS)25년 대전충남",
    "(ITS)25년 전북",
    "(통합)22년 상주영천",
    "(통합)24년 덕송내각",
    "(통합)24년 창원부산",
    "(도로)25년 서울문산",
    "(통합)25년 대구순환",
    "(도로)25년 서울외곽",
    "(도로)25년 대구부산",
    "(통합)21년 서부간선",
    "(통합)18년 옥산오창",
    "(도전)22년 시흥지사TN",
    "(도운)22년 인제양양TN",
    "(도운)22년 육십령TN",
    "(도전)22년 군위지사TN",
    "(국운)22년 금산TN",
    "(지운)22년 용진TN",
    "(국운)22년 밤재TN",
    "(도전)23년 동서울지사TN",
    "(지운)25년 법기TN",
    "(도전)22년 춘천지사TN",
    "(도운)22년 재약산TN",
    "(도전)23년 홍천지사TN",
    "(도전)23년 인천지사TN",
    "(국운)23년 마산TN",
    "(민운)22년 서울춘천TN",
    "(국운)23년 멧둔재TN",
    "(국운)23년 마석TN",
    "(국운)23년 김해TN",
    "(지운)23년 경기남부TN",
    "(국운)24년 백마TN",
    "(국운)23년 진주권역TN",
    "(지운)24년 중원TN",
    "(지운)25년 안민TN",
    "(국운)24년 고덕TN",
    "(도운)24년 남한산성TN",
    "(국운)25년 금산TN",
    "(지운)25년 영월저류지",
    "(지운)25년 용진TN",
    "(국운)25년 밤재TN",
    "(도운)25년 육십령TN",
    "(도전)25년 구례지사TN",
    "(도전)25년 함평지사TN",
    "(도전)25년 청송지사TN",
    "(도전)25년 군위지사TN",
    "(도전)25년 화성지사TN",
    "(도운)25년 신불산TN",
    "(도전)25년 밀양지사TN",
    "(도전)25년 창원지사TN",
    "(도전)25년 진주지사TN",
    "(민운)25년 서울춘천TN",
]


def load_master_names(url: str = None) -> tuple[list, str]:
    global _LAST_MASTER_LOAD_ERROR

    _LAST_MASTER_LOAD_ERROR = ""
    fallback_names = _sanitize_master_names(DEFAULT_MASTER_NAMES)

    if not url:
        return fallback_names, "내장DB"

    try:
        names = fetch_master_from_server(url)
        return names, "서버"
    except Exception as e:
        _LAST_MASTER_LOAD_ERROR = str(e)
        return fallback_names, "내장DB"


def get_last_master_load_error() -> str:
    return _LAST_MASTER_LOAD_ERROR


# ═══════════════════════════════════════════════════════════
#  명칭 매칭 엔진 v3.0
# ═══════════════════════════════════════════════════════════
class NameMatcher:

    EXCLUDE_WORDS = {
        "공사명", "사업명", "현장명", "프로젝트명", "프로젝트",
        "일치여부", "일치", "불일치", "판정", "결과",
        "공사명칭", "명칭", "비고", "구분", "번호",
        "No", "no", "NO", "합계", "소계", "총계",
    }

    # 보고서에서 공사명 뒤에 붙는 노이즈 괄호 패턴
    # (LH), (시공), (시행), (토목), (건축), (전기), (소방) 등
    NOISE_SUFFIX_RE = re.compile(
        r'\((?:LH|시공|시행|토목|건축|기계|조경|감리|설계|PM|발주|원청)\)$'
    )

    def __init__(self, master_names: list):
        self.master_names = list(master_names)
        self.master_set = set(master_names)

        # 순수명칭(접두어 제거) → [공식명칭들]
        self.bare_to_official = {}
        # 마스터에 있는 후미 괄호 패턴 수집 (제거하면 안 되는 것들)
        self.master_suffixes = set()

        for name in master_names:
            bare = self._remove_prefix(name)
            if bare not in self.bare_to_official:
                self.bare_to_official[bare] = []
            self.bare_to_official[bare].append(name)

            # 마스터 순수명칭에 포함된 후미 괄호는 보존 대상
            suffix_match = re.search(r'\([^)]+\)$', bare)
            if suffix_match:
                self.master_suffixes.add(suffix_match.group())

        self.bare_names = list(self.bare_to_official.keys())
        self.min_bare_length = min((len(name) for name in self.bare_names),
                                   default=0)
        sorted_bares = sorted(self.bare_names, key=len, reverse=True)
        sorted_officials = sorted(self.master_names, key=len, reverse=True)
        self._bare_pattern = self._compile_alternation(
            sorted_bares, with_token_boundary=True
        )
        self._official_pattern = self._compile_alternation(sorted_officials)
        self._similarity_cache = {}

    @staticmethod
    def _compile_alternation(items: list, with_token_boundary: bool = False):
        escaped_items = [re.escape(item) for item in items if item]
        if not escaped_items:
            return re.compile(r"(?!x)x")
        body = "|".join(escaped_items)
        if with_token_boundary:
            body = (
                rf'(?<![0-9A-Za-z가-힣])(?:{body})(?![0-9A-Za-z가-힣])'
            )
        return re.compile(body)

    @staticmethod
    def _remove_prefix(name: str) -> str:
        """앞쪽 접두어 (민간), (CM) 등 제거"""
        return re.sub(r'^\([^)]*\)', '', name).strip()

    @staticmethod
    def _has_prefix(name: str) -> bool:
        return bool(re.match(r'^\([^)]+\)', name))

    def _normalize(self, text: str) -> str:
        """
        보고서 명칭 정규화:
        1) 앞쪽 접두어 제거
        2) 뒤쪽 노이즈 괄호 제거 (단, 마스터에 있는 괄호는 유지)
        """
        # 접두어 제거
        result = self._remove_prefix(text)

        # 후미 괄호 확인
        suffix_match = re.search(r'\([^)]+\)$', result)
        if suffix_match:
            suffix = suffix_match.group()
            # 마스터에 있는 후미 괄호면 유지, 아니면 제거
            if suffix not in self.master_suffixes:
                result = result[:suffix_match.start()].strip()

        return result

    def _is_excluded(self, text: str) -> bool:
        clean = text.strip()
        if clean in self.EXCLUDE_WORDS:
            return True
        if len(clean) <= 2:
            return True
        if clean.replace(' ', '').replace('-', '').replace('.', '').isdigit():
            return True
        return False

    def _find_containing_matches(self, normalized: str) -> list:
        """
        정규화된 명칭이 마스터 순수명칭에 포함되거나
        마스터 순수명칭이 정규화된 명칭에 포함되는 경우 찾기
        """
        matches = []
        for bare, officials in self.bare_to_official.items():
            if len(bare) < 4 or len(normalized) < 4:
                continue
            # 양방향 포함 관계 확인
            if bare.startswith(normalized) or normalized.startswith(bare):
                for off in officials:
                    matches.append(off)
        return matches

    @staticmethod
    def _contains_bare_token(text: str, bare: str) -> bool:
        """순수명칭이 더 긴 토큰의 부분문자열이 아닌지 경계 포함 확인"""
        token_re = re.compile(
            rf'(?<![0-9A-Za-z가-힣]){re.escape(bare)}(?![0-9A-Za-z가-힣])'
        )
        return bool(token_re.search(text))

    @staticmethod
    def _extract_prefixed_candidate(text: str, bare: str):
        """
        텍스트 안에서 '(접두어)순수명칭' 형태를 찾아 원문 후보로 반환.
        더 긴 토큰의 부분문자열 매칭은 제외한다.
        """
        prefixed_re = re.compile(
            rf'(\([^)]+\)\s*{re.escape(bare)})(?![0-9A-Za-z가-힣])'
        )
        m = prefixed_re.search(text)
        if m:
            return m.group(1).strip()
        return None

    def find_all_in_text(self, text: str) -> list:
        """텍스트에서 마스터DB와 관련된 모든 문자열을 찾아냄"""
        found = []
        text = text.strip()
        if len(text) < 3:
            return []
        if self._is_excluded(text):
            return []

        # (A) 전체 텍스트가 공식명칭 또는 순수명칭 일치
        if text in self.master_set:
            return [text]
        if text in self.bare_to_official:
            return [text]

        # (B) 정규화 후 일치 확인
        norm = self._normalize(text)
        if norm != text and norm in self.bare_to_official:
            return [text]  # 원본 텍스트를 반환 (정규화 전 형태)
        if len(text) < self.min_bare_length:
            return []

        # (C) 텍스트 안에 공식명칭이 포함
        for match in self._official_pattern.finditer(text):
            official = match.group(0)
            if official not in found:
                found.append(official)

        # (D) 텍스트 안에 순수명칭이 포함
        for match in self._bare_pattern.finditer(text):
            bare = match.group(0)
            officials = self.bare_to_official.get(bare)
            if not officials:
                continue

            already = any(off in found for off in officials)
            if already:
                continue

            # '(접두어)순수명칭'이 있으면 bare 단독 추가를 피하고 원문 후보 유지
            prefixed = self._extract_prefixed_candidate(text, bare)
            if prefixed:
                if prefixed not in found:
                    found.append(prefixed)
                continue

            if bare not in found:
                found.append(bare)

        # (E) 포함 관계 또는 유사도 매칭
        if not found:
            # 정규화 후 포함 관계
            containing = self._find_containing_matches(norm)
            if containing:
                return [text]

            # 유사도 매칭
            best_name, best_score = self._best_similarity(norm)
            if best_score >= 0.7:
                return [text]

        return found

    def _best_similarity(self, text: str):
        """마스터 순수명칭들과 유사도 비교, 최고점 반환"""
        cached = self._similarity_cache.get(text)
        if cached is not None:
            return cached

        best_name = None
        best_score = 0.0
        for bare, officials in self.bare_to_official.items():
            score = SequenceMatcher(None, text, bare).ratio()
            if score > best_score:
                best_score = score
                best_name = officials[0]  # 대표 공식명칭
        result = (best_name, best_score)
        self._similarity_cache[text] = result
        return result

    def check(self, text: str) -> dict:
        """
        단일 문자열을 마스터와 비교하여 판정
        Returns: {input, status, suggestion, issue} or None
        """
        text = text.strip()
        if not text or self._is_excluded(text):
            return None

        # ━━ STEP 0: 완전 일치 ━━
        if text in self.master_set:
            return {
                "input": text, "status": "일치",
                "suggestion": text, "issue": ""
            }

        # ━━ 정규화 ━━
        normalized = self._normalize(text)
        has_prefix = self._has_prefix(text)
        input_prefix = ""
        if has_prefix:
            pm = re.match(r'^\([^)]+\)', text)
            input_prefix = pm.group() if pm else ""

        # ━━ STEP 1: 정규화 후 순수명칭 완전 일치 (후보 1개) ━━
        if normalized in self.bare_to_official:
            officials = self.bare_to_official[normalized]

            if len(officials) == 1:
                reason = self._describe_mismatch(
                    text, normalized, input_prefix, officials[0]
                )
                return {
                    "input": text, "status": "불일치",
                    "suggestion": officials[0],
                    "issue": reason
                }
            else:
                # 방어로직: STEP0에서 걸리지 않았더라도 공식명칭 완전일치면 일치
                for off in officials:
                    if text == off:
                        return {
                            "input": text, "status": "일치",
                            "suggestion": off, "issue": ""
                        }

                # 접두어가 있으면 접두어로 특정 시도
                if has_prefix:
                    matched_official = None
                    for off in officials:
                        if off == input_prefix + normalized:
                            matched_official = off
                            break

                    # 접두어+순수명칭이 특정되면 해당 공식명칭 기준으로 사유 생성
                    if matched_official:
                        reason = self._describe_mismatch(
                            text, normalized, input_prefix, matched_official
                        )
                        return {
                            "input": text, "status": "불일치",
                            "suggestion": matched_official,
                            "issue": reason
                        }

                    # 접두어 불일치
                    return {
                        "input": text, "status": "불일치",
                        "suggestion": " / ".join(officials),
                        "issue": f"접두어 불일치 → 후보: {' / '.join(officials)}"
                    }
                else:
                    return {
                        "input": text, "status": "불일치",
                        "suggestion": " / ".join(officials),
                        "issue": f"접두어 누락 → 후보: {' / '.join(officials)}"
                    }

        # ━━ STEP 2: 포함 관계 (성남복정 → 성남복정1A 등) ━━
        containing = self._find_containing_matches(normalized)
        if containing:
            if len(containing) == 1:
                candidate_bare = self._remove_prefix(containing[0])
                # 예: 성남복정1A1 -> 성남복정1A 는 불완전보다 오탈자에 가까움
                likely_typo = (
                    normalized.startswith(candidate_bare)
                    and len(normalized) - len(candidate_bare) <= 1
                )
                if not likely_typo:
                    return {
                        "input": text, "status": "불일치",
                        "suggestion": containing[0],
                        "issue": f"명칭 불완전 → 공식: {containing[0]}"
                    }
            else:
                display = "\n".join(
                    [f"  {i + 1}. {c}" for i, c in enumerate(containing)]
                )
                return {
                    "input": text, "status": "불일치",
                    "suggestion": " / ".join(containing),
                    "issue": f"특정불가 (유사 {len(containing)}건):\n{display}"
                }

        # ━━ STEP 3: 유사도 매칭 ━━
        best_name, best_score = self._best_similarity(normalized)
        if best_score >= 0.7:
            pct = f"{best_score * 100:.0f}%"
            return {
                "input": text, "status": "불일치",
                "suggestion": best_name,
                "issue": f"오탈자 추정 (유사도 {pct}) → 공식: {best_name}"
            }

        # ━━ STEP 4: 매칭 실패 → 공사명이 아닌 것으로 판단 ━━
        return None

    def _describe_mismatch(self, original: str, normalized: str,
                           input_prefix: str, official: str) -> str:
        """불일치 사유를 구체적으로 설명"""
        reasons = []

        # 접두어 문제
        official_prefix_m = re.match(r'^\([^)]+\)', official)
        official_prefix = official_prefix_m.group() if official_prefix_m else ""

        if not input_prefix:
            reasons.append(f"접두어 누락")
        elif input_prefix != official_prefix:
            reasons.append(
                f"접두어 불일치 ({input_prefix}→{official_prefix})"
            )

        # 후미 노이즈
        if normalized != original and normalized != self._remove_prefix(original):
            # 정규화 과정에서 후미가 제거됨
            pass  # 접두어 제거만 된 경우는 무시
        suffix_in_original = re.search(r'\([^)]+\)$', self._remove_prefix(original))
        if suffix_in_original:
            suffix = suffix_in_original.group()
            official_bare = self._remove_prefix(official)
            if not official_bare.endswith(suffix):
                reasons.append(f"불필요한 후미 {suffix}")

        if not reasons:
            reasons.append("접두어 누락")

        return " + ".join(reasons) + f" → 공식: {official}"


# ═══════════════════════════════════════════════════════════
#  검토 엔진
# ═══════════════════════════════════════════════════════════
class ReviewEngine:

    def __init__(self, matcher: NameMatcher):
        self.matcher = matcher

    @staticmethod
    def _build_full_text_with_offsets(text_items: list) -> tuple[str, list, list]:
        parts = []
        starts = []
        offsets = []
        cursor = 0

        for location, raw_text in text_items:
            text = str(raw_text).strip()
            if not text:
                continue

            parts.append(text)
            start = cursor
            end = start + len(text)
            starts.append(start)
            offsets.append((start, end, location, text))
            cursor = end + 1  # '\n'

        return "\n".join(parts), starts, offsets

    @staticmethod
    def _find_offset_index(starts: list, offsets: list, position: int) -> int:
        idx = bisect.bisect_right(starts, position) - 1
        if idx < 0 or idx >= len(offsets):
            return -1
        start, end, _, _ = offsets[idx]
        if not (start <= position < end):
            return -1
        return idx

    def review_file(self, filepath: str) -> dict:
        filename = os.path.basename(filepath)
        try:
            text_items = extract_text_from_file(filepath)
        except Exception as e:
            return {
                "file": filename, "path": filepath,
                "total": 0, "matched": 0, "mismatched": 0,
                "overall": "오류", "details": [], "error": str(e)
            }

        results = []
        checked_results = {}
        checked_locations = {}

        def consume_candidate(candidate: str, location: str):
            if candidate in checked_results:
                existing = checked_results[candidate]
                if existing["status"] == "불일치":
                    locs = checked_locations[candidate]
                    if location not in locs:
                        locs.append(location)
                        existing["location"] = ", ".join(locs)
                return

            check_result = self.matcher.check(candidate)
            if check_result is None:
                return
            check_result["location"] = location
            results.append(check_result)
            checked_results[candidate] = check_result
            checked_locations[candidate] = [location]

        full_text, starts, offsets = self._build_full_text_with_offsets(text_items)
        if full_text:
            match_events = []
            matched_offset_indices = set()
            for match in self.matcher._official_pattern.finditer(full_text):
                idx = self._find_offset_index(starts, offsets, match.start())
                if idx < 0:
                    continue
                matched_offset_indices.add(idx)
                location = offsets[idx][2]
                match_events.append((match.start(), 0, match.group(0), location))

            for match in self.matcher._bare_pattern.finditer(full_text):
                idx = self._find_offset_index(starts, offsets, match.start())
                if idx < 0:
                    continue
                matched_offset_indices.add(idx)
                _, _, location, source_text = offsets[idx]
                bare = match.group(0)
                prefixed = self.matcher._extract_prefixed_candidate(source_text, bare)
                candidate = prefixed if prefixed else bare
                match_events.append((match.start(), 1, candidate, location))

            match_events.sort(key=lambda item: (item[0], item[1]))

            for _, _, candidate, location in match_events:
                consume_candidate(candidate, location)

            # 정규식 미검출 조각에 대해서만 보조 탐색(유사도 포함) 수행
            for idx, (_, _, location, source_text) in enumerate(offsets):
                if idx in matched_offset_indices:
                    continue
                if len(source_text) > 80:
                    continue
                for candidate in self.matcher.find_all_in_text(source_text):
                    consume_candidate(candidate, location)

        matched = sum(1 for r in results if r["status"] == "일치")
        mismatched = sum(1 for r in results if r["status"] == "불일치")
        total = len(results)

        if total == 0:
            overall = "명칭없음"
        elif mismatched == 0:
            overall = "적합"
        else:
            overall = "검토필요"

        return {
            "file": filename, "path": filepath,
            "total": total, "matched": matched,
            "mismatched": mismatched, "overall": overall,
            "details": results, "error": None
        }


# ═══════════════════════════════════════════════════════════
#  Excel 리포트 생성
# ═══════════════════════════════════════════════════════════
def save_excel_report(all_results: list, output_path: str,
                      master_names: list):
    from openpyxl import Workbook
    from openpyxl.styles import PatternFill, Font, Alignment, Border, Side

    wb = Workbook()
    hdr_fill = PatternFill("solid", fgColor="2F5496")
    hdr_font = Font(color="FFFFFF", bold=True, size=11, name="맑은 고딕")
    ng_fill = PatternFill("solid", fgColor="FFC7CE")
    bdr = Border(left=Side('thin'), right=Side('thin'),
                 top=Side('thin'), bottom=Side('thin'))
    bfont = Font(size=10, name="맑은 고딕")
    ctr = Alignment(horizontal='center', vertical='center')

    ws = wb.active
    ws.title = "불일치목록"

    ws.merge_cells('A1:E1')
    ws['A1'].value = (
        f"공사명칭 불일치 검토 리포트  |  "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )
    ws['A1'].font = Font(bold=True, size=14, name="맑은 고딕")
    ws['A1'].alignment = Alignment(horizontal='center', vertical='center')
    ws.row_dimensions[1].height = 35

    t_found = sum(r["total"] for r in all_results)
    t_match = sum(r["matched"] for r in all_results)
    t_mis = sum(r["mismatched"] for r in all_results)

    ws.merge_cells('A2:E2')
    ws['A2'].value = (
        f"파일 {len(all_results)}개  |  "
        f"발견 {t_found}개  |  "
        f"일치 {t_match}개  |  "
        f"불일치 {t_mis}개"
    )
    ws['A2'].font = Font(size=11, name="맑은 고딕", bold=True)
    ws['A2'].alignment = Alignment(horizontal='center')

    headers = ["파일명", "위치", "보고서 기재 명칭",
               "공식명칭(추천)", "불일치 사유"]
    for ci, h in enumerate(headers, 1):
        cell = ws.cell(row=4, column=ci, value=h)
        cell.fill = hdr_fill
        cell.font = hdr_font
        cell.alignment = ctr
        cell.border = bdr

    row = 5
    for fr in all_results:
        if fr.get("error"):
            ws.cell(row=row, column=1, value=fr["file"]).font = bfont
            ws.cell(row=row, column=3, value="오류").font = bfont
            ws.cell(row=row, column=5, value=fr["error"]).font = bfont
            for c in range(1, 6):
                ws.cell(row=row, column=c).border = bdr
            row += 1
            continue

        # 불일치만 표시
        ng_items = [d for d in fr["details"] if d["status"] == "불일치"]

        if not ng_items and not fr.get("error"):
            continue  # 적합한 파일은 건너뛰기

        for d in ng_items:
            ws.cell(row=row, column=1, value=fr["file"]).font = bfont
            ws.cell(row=row, column=2,
                    value=d.get("location", "")).font = bfont
            c3 = ws.cell(row=row, column=3, value=d["input"])
            c3.font = bfont
            c3.fill = ng_fill
            ws.cell(row=row, column=4,
                    value=d.get("suggestion", "")).font = bfont
            ws.cell(row=row, column=5,
                    value=d.get("issue", "")).font = bfont
            for c in range(1, 6):
                ws.cell(row=row, column=c).border = bdr
            row += 1

    for letter, w in zip('ABCDE', [30, 18, 35, 40, 55]):
        ws.column_dimensions[letter].width = w

    # 마스터 목록 시트
    ws2 = wb.create_sheet("마스터목록")
    for ci, h in enumerate(["No.", "공식 명칭", "접두어", "순수 명칭"], 1):
        cell = ws2.cell(row=1, column=ci, value=h)
        cell.fill = hdr_fill
        cell.font = hdr_font
        cell.alignment = ctr
        cell.border = bdr

    for i, name in enumerate(master_names, 1):
        pm = re.match(r'^\(([^)]*)\)', name)
        prefix = pm.group(1) if pm else ""
        bare = re.sub(r'^\([^)]*\)', '', name).strip()
        ws2.cell(row=i + 1, column=1, value=i).font = bfont
        ws2.cell(row=i + 1, column=2, value=name).font = bfont
        ws2.cell(row=i + 1, column=3, value=prefix).font = bfont
        ws2.cell(row=i + 1, column=4, value=bare).font = bfont
        for c in range(1, 5):
            ws2.cell(row=i + 1, column=c).border = bdr

    for letter, w in zip('ABCD', [8, 45, 15, 35]):
        ws2.column_dimensions[letter].width = w

    wb.save(output_path)

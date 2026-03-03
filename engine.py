"""
공사명칭 검토 엔진 모듈.

목적:
    보고서 파일(PDF/HWP/XLSX 등)에서 추출한 텍스트를
    마스터 DB의 공식 공사명칭과 비교하여 일치/경고/불일치를 판정한다.

주요 기능:
    - 파일 형식별 텍스트 추출
    - 마스터 명칭 로드(내장/서버)
    - 접두어 분리 기반 보정 매칭
    - 검토 결과 Excel 리포트 생성

버전:
    v3.1
"""

import os
import re
import bisect
import queue
import threading
from datetime import datetime
from difflib import SequenceMatcher
from typing import Optional

# 마지막 마스터 로드 실패 사유(서버 동기화 실패 메시지 보관)
_LAST_MASTER_LOAD_ERROR = ""


# ═══════════════════════════════════════════════════════════
#  파일 텍스트 추출 (지연 import)
# ═══════════════════════════════════════════════════════════
# 지원하는 입력 파일 확장자 목록
SUPPORTED_EXTENSIONS = {'.xlsx', '.pdf', '.docx', '.hwp', '.hwpx', '.csv'}

# 마스터DB 공식명칭에 사용되는 접두어 목록
# 긴 접두어부터 매칭하기 위해 사용 시 길이 역순 정렬이 필요하다.
KNOWN_PREFIXES: tuple[str, ...] = (
    "민참.분양", "자체.시공", "자체.시행", "자체.공모", "시행도급",
    "순수내역", "민간", "종평", "적격", "기본", "종심", "턴키",
    "해외", "통합", "도로", "영업", "도전", "도운", "국운",
    "지운", "민운", "민임", "실시", "자체", "민참",
    "CM", "BOT", "BTL", "BTO", "ITS",
)

# PDF/HWP 추출 시 명칭 앞에 붙는 특수기호
STRIP_CHARS = "▶□■❑◆●○◇△▽☆★※→←↑↓·"

# 공식명칭 접두어 파싱용 정규식
PREFIX_PATTERN = re.compile(r'^\(([^)]*)\)\s*(.*)$')


def extract_from_xlsx(filepath: str) -> list:
    """엑셀 파일에서 셀 텍스트를 추출한다.

    Args:
        filepath: `.xlsx` 파일 경로.

    Returns:
        `(위치, 텍스트)` 튜플 목록.
    """
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


def _merge_pdf_prefix_fragments(texts: list) -> list:
    """PDF 추출 결과에서 분리된 접두어 조각을 병합한다.

    PyMuPDF가 표 셀 내부 텍스트를 라인 단위로 분할할 때
    ``(접두어)명칭`` 이 ``접두어텍스트`` + ``(`` + ``)명칭`` 으로
    3조각 나는 경우를 감지하여 ``(접두어)명칭`` 으로 복원한다.

    Args:
        texts: ``(위치, 텍스트)`` 튜플 목록 (extract_from_pdf 원본 결과).

    Returns:
        병합 처리된 ``(위치, 텍스트)`` 튜플 목록.
    """
    if len(texts) < 3:
        return texts

    # 병합 대상으로 소비된 인덱스를 기록한다.
    consumed: set[int] = set()
    merged: list[tuple[str, str]] = []

    i = 0
    while i < len(texts):
        if i in consumed:
            i += 1
            continue

        loc_cur, txt_cur = texts[i]

        # 패턴 감지: 현재 라인이 ")" + 한글/영문/숫자로 시작하는 경우
        if (
            len(txt_cur) >= 2
            and txt_cur[0] == ')'
            and i >= 2
        ):
            _, txt_prev1 = texts[i - 1]  # 바로 직전 라인
            _, txt_prev2 = texts[i - 2]  # 2칸 위 라인

            # 직전 라인이 "(" 이고, 2칸 위 라인 끝에 접두어 텍스트가 있는 경우
            if txt_prev1.strip() == '(':
                # 2칸 위 라인에서 접두어 추출: ": 종심", "- 대상\n: 종심" 등
                # 콜론 뒤의 마지막 토큰을 접두어로 간주
                prefix_text = ""
                colon_match = re.search(r'[:：]\s*(\S+)\s*$', txt_prev2)
                if colon_match:
                    candidate = colon_match.group(1).strip()
                    # KNOWN_PREFIXES에 포함되는지 확인
                    if candidate in KNOWN_PREFIXES:
                        prefix_text = candidate

                if prefix_text:
                    # 순수명칭: ")" 제거한 나머지
                    bare_name = txt_cur[1:].strip()
                    restored = f"({prefix_text}){bare_name}"

                    # 2칸 위 라인에서 접두어 부분을 제거한 텍스트 복원
                    cleaned_prev2 = txt_prev2[:colon_match.start(1)].rstrip()
                    if cleaned_prev2 and cleaned_prev2 != texts[i - 2][1]:
                        # 접두어만 제거된 앞부분이 남으면 유지
                        merged.append((texts[i - 2][0], cleaned_prev2))
                    consumed.add(i - 2)
                    consumed.add(i - 1)  # "(" 라인 소비
                    merged.append((loc_cur, restored))
                    i += 1
                    continue

        if i not in consumed:
            merged.append((loc_cur, txt_cur))
        i += 1

    # consumed에 포함되었지만 merged에서 대체되지 않은 항목 제거
    # (이미 위 로직에서 처리되므로 추가 작업 불필요)

    # ── 비괄호형 접두어 결합 복원 ──
    # PyMuPDF 폴백에서 "접두어+명칭" 결합 형태가 나오는 문제를 보정한다.
    sorted_prefixes = sorted(KNOWN_PREFIXES, key=len, reverse=True)
    restored: list[tuple[str, str]] = []
    for loc, txt in merged:
        stripped = txt.strip()

        # 이미 괄호형 접두어가 있으면 기존 값을 유지한다.
        if stripped.startswith('('):
            restored.append((loc, txt))
            continue

        # 특수기호를 제외한 본문에서 접두어를 긴 순서대로 매칭한다.
        clean = stripped.lstrip(STRIP_CHARS).strip()
        matched_prefix = ""
        for pfx in sorted_prefixes:
            if clean.startswith(pfx) and len(clean) > len(pfx):
                matched_prefix = pfx
                break

        if matched_prefix:
            bare_name = clean[len(matched_prefix):]
            # 앞쪽 특수기호는 유지하고 접두어만 괄호형으로 복원한다.
            leading = stripped[:len(stripped) - len(clean)]
            restored_text = f"{leading}({matched_prefix}){bare_name}"
            restored.append((loc, restored_text))
        else:
            restored.append((loc, txt))

    return restored


def _extract_pages_pdfplumber(
    filepath: str,
    total_pages: int,
    timeout_per_page: float = 2.0,
) -> dict[int, Optional[list[tuple[str, str]]]]:
    """pdfplumber로 전체 페이지를 추출한다. PDF는 한 번만 연다.

    각 페이지 추출에 개별 타임아웃을 적용하며, 타임아웃 또는 오류 발생 시
    해당 페이지는 ``None``으로 기록하여 호출측에서 폴백 처리하도록 한다.

    Args:
        filepath: PDF 파일 경로.
        total_pages: 전체 페이지 수.
        timeout_per_page: 페이지당 추출 타임아웃(초).

    Returns:
        ``{페이지번호: [(위치, 텍스트), ...] 또는 None}`` 딕셔너리.
    """
    import pdfplumber

    results: dict[int, Optional[list[tuple[str, str]]]] = {}

    try:
        pdf = pdfplumber.open(filepath)
    except Exception:
        # pdfplumber로 PDF를 열 수 없으면 전 페이지 None 반환
        return {pg: None for pg in range(1, total_pages + 1)}

    try:
        for page_num in range(1, total_pages + 1):
            if page_num > len(pdf.pages):
                results[page_num] = []
                continue

            page = pdf.pages[page_num - 1]

            # 페이지별 타임아웃 처리
            result_queue: "queue.Queue[tuple[str, object]]" = queue.Queue(maxsize=1)

            def _worker(p=page, pn=page_num):
                try:
                    page_text = p.extract_text() or ""
                    page_texts: list[tuple[str, str]] = []
                    for line_num, line in enumerate(page_text.split('\n'), 1):
                        line = line.strip()
                        if line:
                            page_texts.append((f"P{pn} L{line_num}", line))
                    result_queue.put(("ok", page_texts))
                except Exception:
                    result_queue.put(("error", None))

            worker = threading.Thread(target=_worker, daemon=True)
            worker.start()
            worker.join(timeout_per_page)

            if worker.is_alive():
                # 타임아웃 — 이 페이지는 폴백 대상
                results[page_num] = None
                continue

            if result_queue.empty():
                results[page_num] = None
                continue

            status, payload = result_queue.get_nowait()
            if status == "ok" and payload is not None:
                results[page_num] = payload  # type: ignore[assignment]
            else:
                results[page_num] = None
    finally:
        try:
            pdf.close()
        except Exception:
            pass

    return results


def _extract_pages_pymupdf(
    filepath: str,
    page_nums: list[int],
) -> dict[int, list[tuple[str, str]]]:
    """PyMuPDF로 지정 페이지들의 텍스트를 추출한다. PDF는 한 번만 연다.

    Args:
        filepath: PDF 파일 경로.
        page_nums: 1-based 페이지 번호 목록.

    Returns:
        ``{페이지번호: [(위치, 텍스트), ...]}`` 딕셔너리.
    """
    results: dict[int, list[tuple[str, str]]] = {}
    if not page_nums:
        return results

    try:
        import fitz
    except ImportError:
        return results

    try:
        doc = fitz.open(filepath)
        try:
            for page_num in page_nums:
                if page_num - 1 >= len(doc):
                    results[page_num] = []
                    continue
                page = doc[page_num - 1]
                page_text = page.get_text()
                lines: list[tuple[str, str]] = []
                if page_text:
                    for line_num, line in enumerate(page_text.split('\n'), 1):
                        line = line.strip()
                        if line:
                            lines.append((f"P{page_num} L{line_num}", line))
                results[page_num] = lines
        finally:
            doc.close()
    except Exception:
        pass

    return results


def extract_from_pdf(filepath: str) -> list:
    """PDF 파일에서 줄 단위 텍스트를 추출한다.

    pdfplumber로 전체 페이지를 추출하고(PDF 1회 오픈, 페이지당 타임아웃 2초),
    실패 페이지만 PyMuPDF로 폴백한다(PDF 1회 오픈).
    최종 결과에 조각 병합 후처리를 적용한다.

    Args:
        filepath: ``.pdf`` 파일 경로.

    Returns:
        ``(위치, 텍스트)`` 튜플 목록.
    """
    # ── 페이지 수 확인 ──
    total_pages = 0
    try:
        import fitz
        doc = fitz.open(filepath)
        total_pages = len(doc)
        doc.close()
    except ImportError:
        import pdfplumber
        with pdfplumber.open(filepath) as pdf:
            total_pages = len(pdf.pages)

    if total_pages == 0:
        return []

    # ── 1단계: pdfplumber 전체 추출 (PDF 1회 오픈) ──
    plumber_results = _extract_pages_pdfplumber(
        filepath, total_pages, timeout_per_page=2.0
    )

    # ── 2단계: 실패 페이지 수집 → PyMuPDF 일괄 폴백 (PDF 1회 오픈) ──
    failed_pages = [
        pg for pg in range(1, total_pages + 1)
        if plumber_results.get(pg) is None
    ]
    pymupdf_results = _extract_pages_pymupdf(filepath, failed_pages)

    # ── 3단계: 결과 조립 ──
    texts: list[tuple[str, str]] = []
    for pg in range(1, total_pages + 1):
        page_data = plumber_results.get(pg)
        if page_data is not None:
            texts.extend(page_data)
        else:
            fallback = pymupdf_results.get(pg, [])
            texts.extend(fallback)

    return _merge_pdf_prefix_fragments(texts)


def extract_from_docx(filepath: str) -> list:
    """DOCX 파일에서 문단/표 텍스트를 추출한다.

    Args:
        filepath: `.docx` 파일 경로.

    Returns:
        `(위치, 텍스트)` 튜플 목록.
    """
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
    """HWP(OLE) 파일에서 본문 텍스트를 추출한다.

    Args:
        filepath: `.hwp` 파일 경로.

    Returns:
        `(위치, 텍스트)` 튜플 목록.

    Raises:
        RuntimeError: 파일 파싱 또는 본문 추출에 실패한 경우.
    """
    import struct
    import zlib

    try:
        import olefile
    except ImportError as e:
        raise ImportError("HWP 파싱을 위해 olefile 패키지가 필요합니다.") from e

    texts = []
    line_num = 0

    try:
        if not olefile.isOleFile(filepath):
            raise ValueError("OLE 형식의 .hwp 파일이 아닙니다.")

        with olefile.OleFileIO(filepath) as hwp:
            if not hwp.exists("FileHeader"):
                raise ValueError("FileHeader 스트림을 찾지 못했습니다.")

            header_data = hwp.openstream("FileHeader").read()
            is_compressed = (
                len(header_data) > 36 and (header_data[36] & 1) == 1
            )

            section_nums = []
            for entry in hwp.listdir(streams=True, storages=False):
                if (
                    len(entry) >= 2
                    and entry[0] == "BodyText"
                    and entry[1].startswith("Section")
                ):
                    try:
                        section_nums.append(
                            int(entry[1].replace("Section", ""))
                        )
                    except ValueError:
                        continue
            section_nums = sorted(set(section_nums))

            for snum in section_nums:
                stream_name = f"BodyText/Section{snum}"
                if not hwp.exists(stream_name):
                    continue

                data = hwp.openstream(stream_name).read()
                if is_compressed:
                    try:
                        data = zlib.decompress(data, -15)
                    except zlib.error:
                        continue

                i = 0
                data_len = len(data)
                while i + 4 <= data_len:
                    header_val = struct.unpack_from("<I", data, i)[0]
                    rec_type = header_val & 0x3FF
                    rec_len = (header_val >> 20) & 0xFFF
                    i += 4

                    if rec_len == 0xFFF:
                        if i + 4 > data_len:
                            break
                        rec_len = struct.unpack_from("<I", data, i)[0]
                        i += 4

                    if rec_len < 0 or i + rec_len > data_len:
                        break

                    if rec_type == 67:
                        try:
                            raw = data[i:i + rec_len]
                            text = raw.decode("utf-16-le", errors="ignore")

                            clean_chars = []
                            for ch in text:
                                if ch in ("\r", "\n"):
                                    continue
                                if ord(ch) < 32:
                                    continue
                                clean_chars.append(ch)

                            clean = "".join(clean_chars).strip()
                            if clean:
                                line_num += 1
                                texts.append((f"HWP L{line_num}", clean))
                        except Exception:
                            pass

                    i += rec_len
    except Exception as e:
        raise RuntimeError(f"HWP 읽기 실패: {e}") from e

    if not texts:
        raise RuntimeError("HWP 읽기 실패: 본문 텍스트를 찾지 못했습니다.")
    return texts


def extract_from_hwpx(filepath: str) -> list:
    """HWPX(ZIP+XML) 파일에서 본문 텍스트를 추출한다.

    Args:
        filepath: `.hwpx` 파일 경로.

    Returns:
        `(위치, 텍스트)` 튜플 목록.

    Raises:
        RuntimeError: 파일 열기 또는 XML 파싱에 실패한 경우.
    """
    import zipfile
    from xml.etree import ElementTree as ET

    texts = []
    line_num = 0

    try:
        with zipfile.ZipFile(filepath, 'r') as zf:
            section_files = sorted([
                name for name in zf.namelist()
                if name.startswith('Contents/section') and name.endswith('.xml')
            ])

            if not section_files:
                raise ValueError("Contents/section*.xml을 찾지 못했습니다.")

            for section_file in section_files:
                with zf.open(section_file) as sf:
                    root = ET.parse(sf).getroot()

                    for elem in root.iter():
                        tag = elem.tag
                        if isinstance(tag, str) and '}' in tag:
                            tag = tag.split('}', 1)[1]
                        if tag != 't' or not elem.text:
                            continue

                        text = elem.text.strip()
                        if not text:
                            continue
                        line_num += 1
                        texts.append((f"HWPX L{line_num}", text))

        if not texts:
            raise ValueError("본문 텍스트를 찾지 못했습니다.")
        return texts
    except Exception as e:
        raise RuntimeError(f"HWPX 파일 읽기 실패: {e}")


def extract_from_csv(filepath: str) -> list:
    """CSV 파일을 다중 인코딩으로 읽어 텍스트를 추출한다.

    Args:
        filepath: `.csv` 파일 경로.

    Returns:
        `(위치, 텍스트)` 튜플 목록.

    Raises:
        ValueError: 지원 인코딩으로 디코딩할 수 없는 경우.
    """
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
    """파일 확장자에 따라 적절한 텍스트 추출기를 호출한다.

    Args:
        filepath: 입력 파일 경로.
        include_pdf_tables: 호환용 인자(현재 미사용).

    Returns:
        `(위치, 텍스트)` 튜플 목록.
    """
    # 향후 호환을 위해 인자는 유지한다.
    _ = include_pdf_tables
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".pdf":
        return extract_from_pdf(filepath)

    dispatch = {
        ".xlsx": extract_from_xlsx,
        ".docx": extract_from_docx,
        ".hwp": extract_from_hwp,
        ".hwpx": extract_from_hwpx,
        ".csv": extract_from_csv,
    }
    func = dispatch.get(ext)
    if func is None:
        raise ValueError(f"지원하지 않는 형식: {ext}")
    return func(filepath)


def fetch_master_from_server(url: str, timeout: int = 10) -> tuple[list, Optional[str]]:
    """서버 CSV를 내려받아 마스터 명칭 목록을 로드한다.

    Args:
        url: 마스터 CSV URL.
        timeout: HTTP 타임아웃(초).

    Returns:
        `(정제된 명칭 목록, DB 기준일)` 튜플.
    """
    import csv
    import io
    from email.utils import parsedate_to_datetime
    import requests

    response = requests.get(url, timeout=timeout)
    response.raise_for_status()

    db_date = None
    last_modified = response.headers.get("Last-Modified")
    if last_modified:
        try:
            db_date = parsedate_to_datetime(last_modified).strftime("%Y-%m-%d")
        except (TypeError, ValueError, OverflowError):
            db_date = None

    raw_text = _decode_master_csv_text(response.content)
    reader = csv.reader(io.StringIO(raw_text))
    raw_names = [row[0] for row in reader if row]
    names = _sanitize_master_names(raw_names)
    if len(names) < 10:
        raise ValueError("서버 데이터가 너무 적습니다")

    return names, db_date


# ═══════════════════════════════════════════════════════════
#  기본 마스터 명칭
# ═══════════════════════════════════════════════════════════
def _sanitize_master_names(raw_names: list) -> list:
    """마스터 명칭 원본 목록을 중복 제거/정제한다.

    Args:
        raw_names: 원본 명칭 목록.

    Returns:
        공백/중복/`None`이 제거된 명칭 목록.
    """
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
    """마스터 CSV 바이트를 가능한 인코딩으로 디코딩한다.

    Args:
        raw: CSV 원본 바이트.

    Returns:
        디코딩된 문자열.
    """
    for encoding in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def strip_known_prefix(text: str) -> tuple[str, str]:
    """텍스트 앞에 붙은 알려진 접두어를 분리한다.

    PDF 추출 시 "(종심)포항안동도로"가 "종심포항안동도로"로 합쳐지는
    경우를 처리하기 위한 전처리 함수.

    Args:
        text: 검사 대상 문자열.

    Returns:
        `(접두어, 순수명칭)` 튜플. 접두어 미발견 시 `("", 원본)` 반환.

    Examples:
        "종심포항안동도로" -> ("종심", "포항안동도로")
        "(종심)포항안동도로" -> ("종심", "포항안동도로")
        "▶ 시행도급대전죽동오피스텔" -> ("시행도급", "대전죽동오피스텔")
        "포항안동도로" -> ("", "포항안동도로")
    """
    raw = str(text or "")
    cleaned = raw.strip().strip(STRIP_CHARS).strip()
    if not cleaned:
        return "", ""

    # 1) (접두어)명칭 형태 우선 분리
    pm = PREFIX_PATTERN.match(cleaned)
    if pm:
        prefix = pm.group(1).strip()
        bare = pm.group(2).strip()
        if prefix and bare:
            return prefix, bare
        return "", cleaned

    # 2) 괄호 없는 접두어는 긴 접두어부터 비교하여 오탐을 줄인다.
    for prefix in sorted(KNOWN_PREFIXES, key=len, reverse=True):
        if cleaned.startswith(prefix):
            bare = cleaned[len(prefix):].strip()
            if bare:
                return prefix, bare

    return "", cleaned


def split_official_name(name: str) -> tuple[str, str]:
    """공식명칭을 `(접두어, 순수명칭)`으로 분리한다.

    Args:
        name: 마스터 공식명칭.

    Returns:
        `(접두어, 순수명칭)` 튜플.
    """
    clean = str(name or "").strip()
    pm = PREFIX_PATTERN.match(clean)
    if pm:
        return pm.group(1).strip(), pm.group(2).strip()
    return strip_known_prefix(clean)


def build_bare_name_index(master_names: list) -> dict[str, list[str]]:
    """마스터 명칭으로 순수명칭 인덱스를 생성한다.

    Args:
        master_names: 공식명칭 목록.

    Returns:
        `순수명칭 -> [공식명칭, ...]` 딕셔너리.
    """
    index: dict[str, list[str]] = {}
    for official in master_names:
        _, bare = split_official_name(official)
        if not bare:
            continue
        index.setdefault(bare, []).append(official)
    return index


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


def load_master_names(url: str = None) -> tuple[list, str, Optional[str]]:
    """마스터 명칭 목록을 로드한다.

    Args:
        url: 서버 CSV URL. `None`이면 내장 목록 사용.

    Returns:
        `(명칭 목록, 소스 구분, DB 기준일)` 튜플.
    """
    global _LAST_MASTER_LOAD_ERROR

    _LAST_MASTER_LOAD_ERROR = ""
    fallback_names = _sanitize_master_names(DEFAULT_MASTER_NAMES)

    if not url:
        return fallback_names, "내장", None

    try:
        names, db_date = fetch_master_from_server(url)
        return names, "서버", db_date
    except Exception as e:
        _LAST_MASTER_LOAD_ERROR = str(e)
        return fallback_names, "내장", None


def get_last_master_load_error() -> str:
    """최근 마스터 로드 실패 메시지를 반환한다."""
    return _LAST_MASTER_LOAD_ERROR


# ═══════════════════════════════════════════════════════════
#  명칭 매칭 엔진 v3.0
# ═══════════════════════════════════════════════════════════
class NameMatcher:
    """공사명칭 후보를 마스터 DB와 비교해 판정한다."""

    # 공사명 후보로 볼 필요가 없는 공통 헤더/라벨 텍스트
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
        """매칭에 필요한 인덱스/정규식을 초기화한다.

        Args:
            master_names: 공식명칭 목록.
        """
        self.master_names = list(dict.fromkeys(master_names))
        self.master_set = set(self.master_names)

        # 순수명칭 -> 공식명칭 리스트 인덱스(동일 순수명칭 다건 지원)
        self.bare_to_official = build_bare_name_index(self.master_names)
        self.bare_names = list(self.bare_to_official.keys())
        self.min_bare_length = min((len(name) for name in self.bare_names), default=0)

        # 공식명칭별 접두어를 캐시해 접두어 분기 비교 비용을 줄인다.
        self.official_prefix = {
            official: split_official_name(official)[0] for official in self.master_names
        }

        # 마스터 순수명칭에 존재하는 후미 괄호는 정규화 시 보존한다.
        self.master_suffixes = set()
        for bare in self.bare_names:
            suffix_match = re.search(r'\([^)]+\)$', bare)
            if suffix_match:
                self.master_suffixes.add(suffix_match.group())

        sorted_bares = sorted(self.bare_names, key=len, reverse=True)
        sorted_officials = sorted(self.master_names, key=len, reverse=True)
        self._bare_pattern = self._compile_alternation(sorted_bares, with_token_boundary=True)
        self._official_pattern = self._compile_alternation(sorted_officials)
        self._similarity_cache = {}

    @staticmethod
    def _compile_alternation(items: list, with_token_boundary: bool = False):
        """문자열 목록을 OR 정규식으로 컴파일한다.

        Args:
            items: 패턴으로 만들 문자열 목록.
            with_token_boundary: 토큰 경계 보호 여부.

        Returns:
            컴파일된 정규식 객체.
        """
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
        """앞쪽 접두어를 제거한 순수명칭을 반환한다.

        Deprecated: split_official_name()을 사용할 것.

        Args:
            name: 원본 문자열.

        Returns:
            접두어가 제거된 순수명칭.
        """
        _, bare = split_official_name(name)
        return bare

    @staticmethod
    def _has_prefix(name: str) -> bool:
        """문자열이 `(접두어)`로 시작하는지 확인한다.

        Args:
            name: 검사 대상 문자열.

        Returns:
            접두어 시작 여부.
        """
        return bool(PREFIX_PATTERN.match(name.strip()))

    def _normalize(self, text: str) -> str:
        """비교를 위해 텍스트를 정규화한다.

        Args:
            text: 원본 후보 문자열.

        Returns:
            접두어/노이즈 후미 괄호를 정리한 문자열.
        """
        # 인덱스 키 생성 경로와 조회 경로를 동일화하기 위해 split_official_name()을 사용한다.
        _, result = split_official_name(text.strip())

        # 후미 괄호가 마스터에 없는 노이즈라면 제거한다.
        suffix_match = re.search(r'\([^)]+\)$', result)
        if suffix_match:
            suffix = suffix_match.group()
            if suffix not in self.master_suffixes:
                result = result[:suffix_match.start()].strip()

        return result

    def _is_excluded(self, text: str) -> bool:
        """후보 제외 대상 텍스트인지 판정한다.

        Args:
            text: 검사 대상 문자열.

        Returns:
            제외 대상 여부.
        """
        clean = text.strip()
        if clean in self.EXCLUDE_WORDS:
            return True
        if len(clean) <= 2:
            return True
        if clean.replace(' ', '').replace('-', '').replace('.', '').isdigit():
            return True
        return False

    def _find_containing_matches(self, normalized: str) -> list:
        """포함 관계 기반 후보 공식명칭 목록을 찾는다.

        Args:
            normalized: 정규화된 입력 문자열.

        Returns:
            포함 관계가 성립한 공식명칭 목록.
        """
        matches = []
        for bare, officials in self.bare_to_official.items():
            if len(bare) < 4 or len(normalized) < 4:
                continue
            # 접두어 누락/불완전 입력을 잡기 위해 양방향 포함을 허용한다.
            if bare.startswith(normalized) or normalized.startswith(bare):
                for off in officials:
                    if off not in matches:
                        matches.append(off)
        return matches

    @staticmethod
    def _contains_bare_token(text: str, bare: str) -> bool:
        """순수명칭이 독립 토큰으로 등장하는지 검사한다.

        Args:
            text: 검사 대상 문자열.
            bare: 순수명칭.

        Returns:
            독립 토큰으로 존재하면 `True`.
        """
        token_re = re.compile(
            rf'(?<![0-9A-Za-z가-힣]){re.escape(bare)}(?![0-9A-Za-z가-힣])'
        )
        return bool(token_re.search(text))

    @staticmethod
    def _extract_prefixed_candidate(text: str, bare: str):
        """텍스트에서 `(접두어)순수명칭` 형태 후보를 추출한다.

        Args:
            text: 원문 문자열.
            bare: 순수명칭.

        Returns:
            원문 후보 문자열 또는 `None`.
        """
        prefixed_re = re.compile(
            rf'(\([^)]+\)\s*{re.escape(bare)})(?![0-9A-Za-z가-힣])'
        )
        m = prefixed_re.search(text)
        if m:
            return m.group(1).strip()
        return None

    @staticmethod
    def _is_side_boundary(text: str, index: int, is_left: bool) -> bool:
        """공식명칭 좌/우 경계가 정상인지 확인한다.

        Args:
            text: 원문 텍스트.
            index: 경계 인덱스.
            is_left: 왼쪽 경계 검사 여부.

        Returns:
            시작/끝 또는 공백 경계면 `True`.
        """
        if is_left and index <= 0:
            return True
        if (not is_left) and index >= len(text):
            return True
        ch = text[index - 1] if is_left else text[index]
        return ch.isspace()

    @staticmethod
    def _build_warning_issue(front_ok: bool, back_ok: bool, official: str) -> str:
        """경고 메시지를 규격화한다.

        Args:
            front_ok: 앞 경계 정상 여부.
            back_ok: 뒤 경계 정상 여부.
            official: 대응 공식명칭.

        Returns:
            경고 사유 문자열.
        """
        if not front_ok and not back_ok:
            side = "앞/뒤"
        elif not front_ok:
            side = "앞"
        else:
            side = "뒤"
        return f"경고: 명칭 {side} 부가문자 부착 → 공식: {official}"

    def _check_official_inclusion(self, text: str) -> Optional[dict]:
        """공식명칭 직접 포함 여부와 경계를 함께 판정한다.

        Args:
            text: 검사 대상 문자열.

        Returns:
            일치/경고 판정 dict 또는 `None`.
        """
        warning_result = None
        for match in self._official_pattern.finditer(text):
            official = match.group(0)
            start, end = match.span()
            front_ok = self._is_side_boundary(text, start, is_left=True)
            back_ok = self._is_side_boundary(text, end, is_left=False)

            # 공식명칭이 경계 포함으로 정확히 분리되면 즉시 통과 처리한다.
            if front_ok and back_ok:
                return {
                    "input": text,
                    "status": "일치",
                    "suggestion": official,
                    "issue": "",
                }

            # 앞/뒤에 문자가 붙어 있으면 경고로 기록한다.
            if warning_result is None:
                warning_result = {
                    "input": text,
                    "status": "경고",
                    "suggestion": official,
                    "issue": self._build_warning_issue(front_ok, back_ok, official),
                }
        return warning_result

    @staticmethod
    def _format_ambiguous_issue(candidates: list[str]) -> str:
        """특정불가 사유 문자열을 포맷팅한다.

        Args:
            candidates: 유사 후보 공식명칭 목록.

        Returns:
            리포트 표기용 특정불가 사유 문자열.
        """
        display = " ".join([f"{i + 1}. {name}" for i, name in enumerate(candidates)])
        return f"특정불가 (유사 {len(candidates)}건): {display}"

    def find_all_in_text(self, text: str) -> list:
        """텍스트에서 마스터DB와 연관된 후보 문자열을 찾는다.

        Args:
            text: 원문 텍스트.

        Returns:
            후보 문자열 목록.
        """
        found = []
        text = text.strip()
        if len(text) < 3:
            return []
        if self._is_excluded(text):
            return []

        # (A) 전체 텍스트가 공식명칭/순수명칭과 동일한 경우
        if text in self.master_set:
            return [text]
        if text in self.bare_to_official:
            return [text]

        # (B) 정규화 후 순수명칭이 맞으면 원문을 후보로 올린다.
        norm = self._normalize(text)
        if norm != text and norm in self.bare_to_official:
            return [text]
        if len(text) < self.min_bare_length:
            return []

        # (C) 텍스트 안의 공식명칭 직접 포함 후보
        for match in self._official_pattern.finditer(text):
            official = match.group(0)
            if official not in found:
                found.append(official)

        # (D) 텍스트 안의 순수명칭 포함 후보
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

        # (E) 정규식으로 못 잡은 경우 포함/유사도 후보를 보조 탐색한다.
        if not found:
            containing = self._find_containing_matches(norm)
            if containing:
                return [text]

            best_name, best_score = self._best_similarity(norm)
            if best_score >= 0.7:
                return [text]

        return found

    def _best_similarity(self, text: str):
        """순수명칭 기준 최고 유사도 공식명칭을 찾는다.

        Args:
            text: 비교 대상 문자열.

        Returns:
            `(공식명칭, 유사도)` 튜플.
        """
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
        """단일 문자열을 마스터 명칭 규칙으로 판정한다.

        Args:
            text: 검사 대상 문자열.

        Returns:
            `{input, status, suggestion, issue}` 형태 dict 또는 `None`.
        """
        text = str(text or "").strip()
        if not text or self._is_excluded(text):
            return None

        # STEP 1: 공식명칭 포함 + 경계 판정(통과/경고)
        inclusion_result = self._check_official_inclusion(text)
        if inclusion_result:
            return inclusion_result

        # STEP 2: 접두어 분리 매칭
        normalized = self._normalize(text)
        split_prefix, split_bare = strip_known_prefix(text)
        split_bare = split_bare or normalized
        stripped_input = text.strip().strip(STRIP_CHARS).strip()
        is_direct_prefix_form = (
            bool(split_prefix)
            and not self._has_prefix(text)
            and stripped_input.startswith(split_prefix)
        )

        if split_bare in self.bare_to_official:
            officials = self.bare_to_official[split_bare]
            if len(officials) == 1:
                official = officials[0]
                official_prefix = self.official_prefix.get(official, "")

                if not split_prefix:
                    return {
                        "input": text,
                        "status": "불일치",
                        "suggestion": official,
                        "issue": f"접두어 누락 → 공식: {official}",
                    }

                if split_prefix == official_prefix:
                    return {
                        "input": text,
                        "status": "불일치",
                        "suggestion": official,
                        "issue": f"접두어 분리매칭 → 공식: {official}",
                    }

                return {
                    "input": text,
                    "status": "불일치",
                    "suggestion": official,
                    "issue": f"접두어 불일치 ({split_prefix}->{official_prefix}) → 공식: {official}",
                }

            return {
                "input": text,
                "status": "불일치",
                "suggestion": " / ".join(officials),
                "issue": self._format_ambiguous_issue(officials),
            }

        # 괄호 없는 "접두어+명칭" 형태는 접두어 분리 유사매칭을 우선 설명한다.
        if is_direct_prefix_form and len(split_bare) >= 4:
            best_split_name, best_split_score = self._best_similarity(split_bare)
            if (
                best_split_name
                and best_split_score >= 0.9
                and self.official_prefix.get(best_split_name, "") == split_prefix
            ):
                return {
                    "input": text,
                    "status": "불일치",
                    "suggestion": best_split_name,
                    "issue": f"접두어 분리매칭 → 공식: {best_split_name}",
                }

        # STEP 3: 기존 포함 관계/유사도 비교
        containing = self._find_containing_matches(normalized)
        if containing:
            if len(containing) == 1:
                # 포함 비교도 인덱스와 동일한 접두어 분리 경로를 사용한다.
                _, candidate_bare = split_official_name(containing[0])
                candidate_bare = candidate_bare.replace(" ", "")
                compare_text = normalized.replace(" ", "")

                # "공식명칭+1글자" 수준은 오탈자 가능성이 높아 유사도 단계로 넘긴다.
                likely_typo = (
                    compare_text.startswith(candidate_bare)
                    and len(compare_text) - len(candidate_bare) <= 1
                )
                if not likely_typo:
                    return {
                        "input": text,
                        "status": "불일치",
                        "suggestion": containing[0],
                        "issue": f"명칭 불완전 → 공식: {containing[0]}",
                    }
            else:
                return {
                    "input": text,
                    "status": "불일치",
                    "suggestion": " / ".join(containing),
                    "issue": self._format_ambiguous_issue(containing),
                }

        best_name, best_score = self._best_similarity(normalized)
        if best_score >= 0.7:
            pct = f"{best_score * 100:.0f}%"
            return {
                "input": text,
                "status": "불일치",
                "suggestion": best_name,
                "issue": f"오탈자 추정 (유사도 {pct}) → 공식: {best_name}",
            }

        # 공사명으로 보기 어려운 텍스트는 결과 목록에서 제외한다.
        return None

# ═══════════════════════════════════════════════════════════
#  검토 엔진
# ═══════════════════════════════════════════════════════════
class ReviewEngine:
    """파일 단위로 후보 명칭을 수집하고 판정 결과를 집계한다."""

    def __init__(self, matcher: NameMatcher):
        """검토 엔진을 초기화한다.

        Args:
            matcher: 명칭 판정기 인스턴스.
        """
        self.matcher = matcher

    @staticmethod
    def _build_full_text_with_offsets(text_items: list) -> tuple[str, list, list]:
        """텍스트 조각 목록을 단일 문자열과 오프셋 테이블로 변환한다.

        Args:
            text_items: `(위치, 텍스트)` 목록.

        Returns:
            `(결합문자열, 시작오프셋목록, 오프셋메타목록)` 튜플.
        """
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
        """결합 문자열 위치를 원본 조각 인덱스로 역매핑한다.

        Args:
            starts: 각 조각 시작 오프셋 목록.
            offsets: 조각 메타 목록.
            position: 결합 문자열 내 문자 위치.

        Returns:
            대응 조각 인덱스. 없으면 `-1`.
        """
        idx = bisect.bisect_right(starts, position) - 1
        if idx < 0 or idx >= len(offsets):
            return -1
        start, end, _, _ = offsets[idx]
        if not (start <= position < end):
            return -1
        return idx

    @staticmethod
    def _extend_official_candidate(source_text: str, start: int, end: int) -> str:
        """공식명칭 주변 부착 문자를 포함한 후보를 생성한다.

        Args:
            source_text: 원본 줄 텍스트.
            start: 공식명칭 시작 인덱스(줄 기준).
            end: 공식명칭 끝 인덱스(줄 기준, exclusive).

        Returns:
            경계 부착 여부를 판정할 수 있도록 확장된 후보 문자열.
        """
        left = start
        right = end

        # 경고 판정을 위해 공백이 아닌 좌/우 1글자를 함께 포함한다.
        if left > 0 and not source_text[left - 1].isspace():
            left -= 1
        if right < len(source_text) and not source_text[right].isspace():
            right += 1
        return source_text[left:right].strip()

    def review_file(self, filepath: str) -> dict:
        """단일 파일을 검토해 공사명칭 판정 결과를 반환한다.

        Args:
            filepath: 입력 파일 경로.

        Returns:
            파일 단위 집계 결과 dict.
        """
        filename = os.path.basename(filepath)
        try:
            text_items = extract_text_from_file(filepath)
        except Exception as e:
            # 파일 파싱 실패는 전체 검토 결과를 오류 상태로 반환한다.
            return {
                "file": filename, "path": filepath,
                "total": 0, "matched": 0, "mismatched": 0,
                "overall": "오류", "details": [], "error": str(e)
            }

        results = []
        checked_results = {}
        checked_locations = {}

        def consume_candidate(candidate: str, location: str):
            """후보를 판정하고 중복 결과를 병합한다."""
            if candidate in checked_results:
                existing = checked_results[candidate]
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

            # 공식명칭 직접 매칭은 경고 판정을 위해 좌/우 부착 문자를 포함한다.
            for match in self.matcher._official_pattern.finditer(full_text):
                idx = self._find_offset_index(starts, offsets, match.start())
                if idx < 0:
                    continue
                matched_offset_indices.add(idx)
                line_start, _, location, source_text = offsets[idx]
                local_start = match.start() - line_start
                local_end = local_start + len(match.group(0))
                candidate = self._extend_official_candidate(source_text, local_start, local_end)
                match_events.append((match.start(), 0, candidate, location))

            # 순수명칭 매칭은 기존 방식 유지(접두어/유사도 단계로 연결).
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

            # 정규식 미검출 조각에 대해서만 보조 탐색(포함/유사도)을 수행한다.
            for idx, (_, _, location, source_text) in enumerate(offsets):
                if idx in matched_offset_indices:
                    continue
                if len(source_text) > 80:
                    continue
                for candidate in self.matcher.find_all_in_text(source_text):
                    consume_candidate(candidate, location)

        matched = sum(1 for r in results if r["status"] == "일치")
        mismatched = sum(1 for r in results if r["status"] == "불일치")
        warnings = sum(1 for r in results if r["status"] == "경고")
        total = len(results)

        if total == 0:
            overall = "명칭없음"
        elif mismatched == 0 and warnings == 0:
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
    """검토 결과를 Excel 리포트로 저장한다.

    Args:
        all_results: 파일별 검토 결과 목록.
        output_path: 저장할 xlsx 경로.
        master_names: 마스터 공식명칭 목록.
    """
    from openpyxl import Workbook
    from openpyxl.styles import PatternFill, Font, Alignment, Border, Side

    # 헤더/본문 스타일 정의
    wb = Workbook()
    hdr_fill = PatternFill("solid", fgColor="2F5496")
    hdr_font = Font(color="FFFFFF", bold=True, size=11, name="맑은 고딕")
    ng_fill = PatternFill("solid", fgColor="FFC7CE")
    warn_fill = PatternFill("solid", fgColor="FFF2CC")
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

        # 불일치/경고만 표시(일치는 리포트에서 제외)
        ng_items = [
            d for d in fr["details"]
            if d["status"] in ("불일치", "경고")
        ]

        if not ng_items and not fr.get("error"):
            continue  # 적합한 파일은 건너뛰기

        for d in ng_items:
            ws.cell(row=row, column=1, value=fr["file"]).font = bfont
            ws.cell(row=row, column=2,
                    value=d.get("location", "")).font = bfont
            c3 = ws.cell(row=row, column=3, value=d["input"])
            c3.font = bfont

            # 경고는 노랑, 불일치는 빨강으로 강조한다.
            if d["status"] == "경고":
                c3.fill = warn_fill
            else:
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
        prefix, bare = split_official_name(name)
        ws2.cell(row=i + 1, column=1, value=i).font = bfont
        ws2.cell(row=i + 1, column=2, value=name).font = bfont
        ws2.cell(row=i + 1, column=3, value=prefix).font = bfont
        ws2.cell(row=i + 1, column=4, value=bare).font = bfont
        for c in range(1, 5):
            ws2.cell(row=i + 1, column=c).border = bdr

    for letter, w in zip('ABCD', [8, 45, 15, 35]):
        ws2.column_dimensions[letter].width = w

    wb.save(output_path)


def generate_highlight_snapshots(
    filepath: str,
    ng_items: list[dict],
    scale: float = 2.0,
) -> list[tuple[int, bytes]]:
    """PDF 파일에서 불일치 위치를 하이라이트한 페이지 이미지를 생성한다.

    Args:
        filepath: PDF 파일 경로.
        ng_items: 불일치 항목 목록(`{"input": str, "location": str, ...}`).
        scale: 이미지 렌더링 배율(기본 2배).

    Returns:
        ``(페이지번호, PNG바이트)`` 튜플 목록. 생성 실패 시 빈 목록.
    """
    try:
        import fitz
    except ImportError:
        return []

    if os.path.splitext(filepath)[1].lower() != ".pdf":
        return []

    # 페이지별 검색어를 묶어 페이지 렌더링을 최소화한다.
    page_targets: dict[int, list[str]] = {}
    for item in ng_items:
        location = item.get("location", "")
        m = re.match(r'P(\d+)', location)
        if not m:
            continue
        page_num = int(m.group(1))

        # 검색어는 앞쪽 특수기호를 제거해 검색 성공률을 높인다.
        search_text = item.get("input", "").strip()
        search_text = search_text.lstrip(STRIP_CHARS).strip()
        if not search_text or len(search_text) < 2:
            continue
        page_targets.setdefault(page_num, []).append(search_text)

    if not page_targets:
        return []

    results: list[tuple[int, bytes]] = []
    try:
        doc = fitz.open(filepath)
        try:
            mat = fitz.Matrix(scale, scale)
            for page_num in sorted(page_targets.keys()):
                if page_num - 1 >= len(doc):
                    continue
                page = doc[page_num - 1]
                targets = page_targets[page_num]

                for search_text in targets:
                    # 1차: 원문 문자열 검색
                    rects = page.search_for(search_text)

                    # 2차: 접두어 제거 순수명칭 검색
                    if not rects:
                        _, bare = split_official_name(search_text)
                        if bare and bare != search_text:
                            rects = page.search_for(bare)

                    # 3차: 앞 4글자 검색
                    if not rects and len(search_text) >= 4:
                        rects = page.search_for(search_text[:4])

                    for rect in rects:
                        expanded = fitz.Rect(
                            rect.x0 - 2, rect.y0 - 2,
                            rect.x1 + 20, rect.y1 + 2
                        )
                        # 가시성을 위해 빨간 테두리 + 노란 하이라이트를 함께 표시한다.
                        annot = page.add_rect_annot(expanded)
                        annot.set_colors(stroke=(1, 0, 0))
                        annot.set_border(width=2)
                        annot.update()

                        highlight = page.add_highlight_annot(expanded)
                        highlight.update()

                pix = page.get_pixmap(matrix=mat)
                results.append((page_num, pix.tobytes("png")))
        finally:
            doc.close()
    except Exception:
        pass

    return results

#!/usr/bin/env python3
"""소스 무결성 검증 스크립트"""
import sys, os, re, importlib, inspect

os.chdir('/opt/name-checker')
sys.path.insert(0, '.')

errors = []
warnings = []
passed = 0

def check(condition, msg):
    global passed
    if condition:
        passed += 1
        print(f"  ✅ {msg}")
    else:
        errors.append(msg)
        print(f"  ❌ {msg}")

def warn(condition, msg):
    global passed
    if condition:
        passed += 1
        print(f"  ✅ {msg}")
    else:
        warnings.append(msg)
        print(f"  ⚠️  {msg}")

print("=" * 60)
print("공사명칭 검토기 소스 무결성 검증")
print("=" * 60)

print("\n[1] 파일 존재 확인")
for f in ['engine.py','main.py','static/index.html','app_icon.ico','app_icon.png']:
    check(os.path.exists(f), f"파일 존재: {f}")

print("\n[2] engine.py 모듈 검증")
try:
    import engine
    check(True, "engine.py 임포트 성공")
except Exception as e:
    check(False, f"engine.py 임포트 실패: {e}")
    sys.exit(1)

for attr in ['NameMatcher','ReviewEngine','extract_text_from_file','load_master_names',
             'save_excel_report','generate_highlight_snapshots','split_official_name',
             'KNOWN_PREFIXES','DEFAULT_MASTER_NAMES']:
    check(hasattr(engine, attr), f"{attr} 존재")

print("\n[3] 마스터 데이터 검증")
raw = engine.load_master_names()
master = raw[0] if isinstance(raw, tuple) else raw
check(master is not None and len(master) > 100, f"마스터 {len(master) if master else 0}건")

print("\n[4] NameMatcher 검증")
matcher = engine.NameMatcher(master)
check(hasattr(matcher, 'master_names'), "master_names 속성")
check(len(matcher.master_names) == len(master), f"길이 일치: {len(matcher.master_names)}")

print("\n[5] ReviewEngine.review_file 검증")
eng = engine.ReviewEngine(matcher)
src = inspect.getsource(eng.review_file)
for kw, desc in [('fitz','PyMuPDF 사용'),('anchor','토큰 앵커 매칭'),
                  ('norm_to_raw','norm→raw 매핑'),('KNOWN_PREFIXES','접두어 참조'),
                  ('일치','일치 상태'),('불일치','불일치 상태'),('명칭없음','명칭없음 처리'),
                  ('적합','적합 처리'),('검토필요','검토필요 처리')]:
    check(kw in src, f"review_file: {desc}")

print("\n[6] save_excel_report 시그니처")
params = list(inspect.signature(engine.save_excel_report).parameters.keys())
for p in ['all_results','output_path','master_names']:
    check(p in params, f"파라미터: {p}")

print("\n[7] main.py 검증")
with open('main.py','r',encoding='utf-8') as f:
    msrc = f.read()
for kw, desc in [('FastAPI','FastAPI'),('/api/review','검토 API'),('/api/review/excel','Excel API'),
                  ('app_icon','아이콘 라우트'),('file.filename','원본 파일명'),
                  ('BackgroundTask','BackgroundTask'),('[result]','리스트 전달'),
                  ('matcher.master_names','마스터 전달')]:
    check(kw in msrc, f"main.py: {desc}")

print("\n[8] index.html 검증")
with open('static/index.html','r',encoding='utf-8') as f:
    hsrc = f.read()
for kw, desc in [('dropZone','드래그앤드롭'),('multiple','다중 파일'),('btnStart','검토 시작'),
                  ('btnExcel','Excel 다운로드'),('btnClear','초기화'),('centerHourglass','모래시계'),
                  ('app_icon.ico','파비콘'),('app_icon.png','로고'),('allResults','결과 변수'),
                  ('/api/review/excel','Excel API 호출')]:
    check(kw in hsrc, f"index.html: {desc}")

print("\n[9] 시스템 서비스 검증")
svc = '/etc/systemd/system/name-checker.service'
if os.path.exists(svc):
    with open(svc,'r') as f:
        ss = f.read()
    for kw, desc in [('uvicorn','uvicorn'),('8000','포트 8000'),('Restart=always','자동 재시작')]:
        check(kw in ss, f"서비스: {desc}")
else:
    check(False, "서비스 파일 존재")

print("\n[10] 필수 패키지 검증")
for pkg in ['fitz','pdfplumber','rapidfuzz','openpyxl','fastapi','uvicorn','PIL']:
    try:
        importlib.import_module(pkg)
        check(True, f"패키지: {pkg}")
    except ImportError:
        check(False, f"패키지 미설치: {pkg}")

print("\n" + "=" * 60)
total = passed + len(errors) + len(warnings)
print(f"검증 결과: ✅ {passed}/{total} 통과  ❌ {len(errors)}건 실패  ⚠️  {len(warnings)}건 경고")
if errors:
    print("\n실패 항목:")
    for e in errors:
        print(f"  ❌ {e}")
if warnings:
    print("\n경고 항목:")
    for w in warnings:
        print(f"  ⚠️  {w}")
print("=" * 60)

"""
공사명칭 검토기 v1.0 (GUI)
"""

import os
import queue
import sys
import threading
import io
from datetime import datetime
from time import perf_counter

import customtkinter as ctk
import tkinter as tk
import tkinter.font as tkfont
from tkinter import filedialog, messagebox
try:
    from PIL import Image, ImageTk
except ImportError:
    Image = None
    ImageTk = None

try:
    import windnd
except ImportError:
    windnd = None

from engine import (
    SUPPORTED_EXTENSIONS,
    generate_highlight_snapshots,
    get_last_master_load_error,
    load_master_names,
    NameMatcher,
    ReviewEngine,
    save_excel_report,
)

MASTER_DB_URL = "https://www.krindus.co.kr/resources/upload/itdata/MasterDB.csv"
APP_VERSION = "v1.0"


def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


class App(ctk.CTk):

    def __init__(self):
        super().__init__()

        self.title(f"공사명칭 검토기 {APP_VERSION}")
        self.geometry("1150x780")
        self.minsize(950, 650)

        # 윈도우 아이콘 설정
        self._icon_path = resource_path('app_icon.ico')
        if os.path.exists(self._icon_path):
            self.iconbitmap(self._icon_path)

        ctk.set_appearance_mode("light")
        self.PREFERRED_FONTS = [
            "맑은 고딕", "Malgun Gothic", "Segoe UI",
            "Noto Sans CJK KR", "NanumGothic",
            "Arial Unicode MS", "TkDefaultFont"
        ]
        self.SYSTEM_FONT = next(
            (f for f in self.PREFERRED_FONTS if f in tkfont.families()),
            "TkDefaultFont"
        )
        self.DEFAULT_FONT = ctk.CTkFont(
            family=self.SYSTEM_FONT, size=12, weight="normal"
        )
        self.BOLD_FONT = ctk.CTkFont(
            family=self.SYSTEM_FONT, size=14, weight="bold"
        )
        self.TITLE_FONT = ctk.CTkFont(
            family=self.SYSTEM_FONT, size=22, weight="bold"
        )
        self.SMALL_FONT = ctk.CTkFont(
            family=self.SYSTEM_FONT, size=11, weight="normal"
        )
        self.LARGE_FONT = ctk.CTkFont(
            family=self.SYSTEM_FONT, size=15, weight="normal"
        )
        self.MONO_FONT = ctk.CTkFont(family="Consolas", size=12)

        self.tk.call('tk', 'scaling', 1.0)
        ctk.set_widget_scaling(1.1)

        print(f"시스템 폰트 적용: {self.SYSTEM_FONT}")
        ctk.set_default_color_theme("blue")

        self.master_names, self.db_source, self.db_date = load_master_names(MASTER_DB_URL)
        self.is_server_synced = self.db_source == "서버"
        self._startup_db_error = get_last_master_load_error()
        self.matcher = NameMatcher(self.master_names)
        self.engine = ReviewEngine(self.matcher)

        self.file_paths = []
        self.all_results = []
        self.is_reviewing = False
        self.is_syncing = False
        self._drop_queue = queue.Queue()
        self._sync_queue = queue.Queue()

        self._build_ui()
        self._pending_progress = 0.0
        self.bind("<<UpdateProgress>>", self._on_update_progress)
        self._poll_drop_queue()
        self._poll_sync_queue()

        if self.db_source == "서버":
            self._log(f"초기 DB 로드 완료: 서버 ({len(self.master_names)}개)")
        else:
            if self._startup_db_error:
                self._log(
                    "초기 DB 서버 로드 실패: "
                    f"{self._startup_db_error} (내장DB 사용)"
                )
            else:
                self._log(f"초기 DB 로드: 내장DB ({len(self.master_names)}개)")

    def _build_ui(self):

        # 상단
        top = ctk.CTkFrame(self, fg_color="#2F5496",
                           corner_radius=0, height=55)
        top.pack(fill="x")
        top.pack_propagate(False)

        ctk.CTkLabel(
            top, text="  공사명칭 검토기",
            font=self.TITLE_FONT,
            text_color="white"
        ).pack(side="left", padx=15, pady=12)

        self.help_btn = ctk.CTkButton(
            top,
            text="도움말",
            width=80,
            height=30,
            fg_color="#4472C4",
            hover_color="#3A5FAE",
            text_color="white",
            font=self.DEFAULT_FONT,
            command=self._show_help,
        )
        self.help_btn.pack(side="right", padx=15, pady=12)

        # 메인
        main = ctk.CTkFrame(self, fg_color="transparent")
        main.pack(fill="both", expand=True, padx=12, pady=8)

        # 좌측
        left = ctk.CTkFrame(main, width=380)
        left.pack(side="left", fill="both", padx=(0, 6))
        left.pack_propagate(False)

        ctk.CTkLabel(
            left, text="검토 대상 파일",
            font=self.BOLD_FONT
        ).pack(pady=(10, 6), padx=10, anchor="w")

        bf = ctk.CTkFrame(left, fg_color="transparent")
        bf.pack(fill="x", padx=10)

        bf.grid_columnconfigure((0, 1), weight=1)

        self.add_file_btn = ctk.CTkButton(
            bf, text="파일 추가", width=150, height=32,
            command=self._add_files
        )
        self.add_file_btn.grid(row=0, column=0, padx=4, pady=2, sticky="ew")

        self.add_folder_btn = ctk.CTkButton(
            bf, text="폴더 추가", width=150, height=32,
            command=self._add_folder
        )
        self.add_folder_btn.grid(row=0, column=1, padx=4, pady=2, sticky="ew")

        self.remove_selected_btn = ctk.CTkButton(
            bf, text="선택 삭제", width=150, height=32,
            fg_color="#6C757D", hover_color="#5A6268", text_color="white",
            command=self._remove_selected
        )
        self.remove_selected_btn.grid(
            row=1, column=0, padx=4, pady=2, sticky="ew"
        )

        self.clear_files_btn = ctk.CTkButton(
            bf, text="전체 삭제", width=150, height=32,
            fg_color="#DC3545", hover_color="#C82333", text_color="white",
            command=self._clear_files
        )
        self.clear_files_btn.grid(row=1, column=1, padx=4, pady=2, sticky="ew")

        lf = ctk.CTkFrame(left, fg_color="transparent")
        lf.pack(fill="both", expand=True, padx=10, pady=8)

        sb = tk.Scrollbar(lf)
        sb.pack(side="right", fill="y", padx=(0, 4), pady=(8, 4))

        self.file_listbox = tk.Listbox(
            lf, font=(self.SYSTEM_FONT, 11),
            selectmode=tk.EXTENDED, activestyle="none",
            bg="white",
            yscrollcommand=sb.set
        )
        self.file_listbox.pack(
            fill="both", expand=True, padx=(4, 2), pady=(8, 4)
        )
        sb.config(command=self.file_listbox.yview)

        self._build_drop_hint(lf)

        if windnd is not None:
            try:
                windnd.hook_dropfiles(
                    self.file_listbox,
                    func=self._on_drop_files,
                    force_unicode=True
                )
                windnd.hook_dropfiles(
                    lf,
                    func=self._on_drop_files,
                    force_unicode=True
                )
            except Exception:
                pass

        self.count_label = ctk.CTkLabel(
            left, text="파일 0개", font=self.SMALL_FONT
        )
        self.count_label.pack(padx=10, pady=(0, 2), anchor="w")

        self.support_label = ctk.CTkLabel(
            left,
            text="지원 형식: HWP · HWPX · PDF · XLSX · DOCX · CSV",
            font=self.LARGE_FONT,
            text_color="gray"
        )
        self.support_label.pack(padx=10, pady=(0, 5), anchor="w")

        # 우측
        right = ctk.CTkFrame(main)
        right.pack(side="right", fill="both", expand=True)

        ctk.CTkLabel(
            right, text="불일치 검토 결과",
            font=self.BOLD_FONT
        ).pack(pady=(10, 6), padx=10, anchor="w")

        self.result_box = ctk.CTkTextbox(
            right,
            font=self.MONO_FONT,
            wrap="word"
        )
        self.result_box.pack(fill="both", expand=True,
                             padx=10, pady=(0, 10))

        # 하단
        bottom = ctk.CTkFrame(self, fg_color="transparent")
        bottom.pack(fill="x", padx=12, pady=(0, 12))

        self.progress = ctk.CTkProgressBar(bottom, height=12)
        self.progress.pack(fill="x", pady=(0, 8))
        self.progress.set(0)

        self.db_source_label = ctk.CTkLabel(
            bottom,
            text="DB상태 : -",
            font=self.SMALL_FONT,
            text_color="#2F5496"
        )
        self.sync_db_btn = ctk.CTkButton(
            bottom,
            text="DB동기",
            width=96,
            height=32,
            fg_color="#6C757D",
            hover_color="#5A6268",
            text_color="white",
            command=self._sync_db
        )
        self.sync_db_btn.pack(side="left", padx=(0, 8))
        self.db_source_label.pack(side="left", padx=(0, 0))
        self.status_label = ctk.CTkLabel(
            bottom,
            text="파일을 추가한 후 [검토 시작]을 눌러주세요.",
            font=self.SMALL_FONT
        )
        self.status_label.pack(side="left", padx=(8, 0), fill="x", expand=True)

        self.report_btn = ctk.CTkButton(
            bottom, text="리포트 저장 (Excel)",
            width=170, height=38,
            fg_color="#28A745", hover_color="#218838",
            font=self.BOLD_FONT,
            command=self._save_report
        )
        self.report_btn.pack(side="right", padx=(8, 0))

        self.snapshot_btn = ctk.CTkButton(
            bottom, text="스냅샷 보기",
            width=130, height=38,
            fg_color="#17A2B8", hover_color="#138496",
            font=self.BOLD_FONT,
            command=self._show_snapshots
        )
        self.snapshot_btn.pack(side="right", padx=(8, 0))

        self.review_btn = ctk.CTkButton(
            bottom, text="검토 시작",
            width=140, height=38,
            font=self.BOLD_FONT,
            command=self._start_review
        )
        self.review_btn.pack(side="right")

        self._refresh_count()
        self._update_db_source_label()

    # ── 파일 관리 ──
    def _add_files(self):
        if self.is_reviewing:
            return
        types = [
            ("지원 형식", "*.hwp *.hwpx *.pdf *.xlsx *.docx *.csv"),
            ("HWP", "*.hwp"), ("HWPX", "*.hwpx"), ("PDF", "*.pdf"),
            ("Excel", "*.xlsx"), ("Word", "*.docx"), ("CSV", "*.csv"),
        ]
        paths = filedialog.askopenfilenames(
            title="파일 선택", filetypes=types
        )
        for p in paths:
            if p not in self.file_paths:
                self.file_paths.append(p)
                self.file_listbox.insert(
                    tk.END, self._format_listbox_item(os.path.basename(p))
                )
        self._refresh_count()

    def _add_folder(self):
        if self.is_reviewing:
            return
        folder = filedialog.askdirectory(title="폴더 선택")
        if not folder:
            return
        added = 0
        for root, _, files in os.walk(folder):
            for f in files:
                ext = os.path.splitext(f)[1].lower()
                if ext in SUPPORTED_EXTENSIONS:
                    fp = os.path.join(root, f)
                    if fp not in self.file_paths:
                        self.file_paths.append(fp)
                        self.file_listbox.insert(
                            tk.END, self._format_listbox_item(f)
                        )
                        added += 1
        self._refresh_count()
        if added == 0:
            messagebox.showinfo("알림", "지원되는 파일이 없습니다.")

    def _remove_selected(self):
        if self.is_reviewing:
            return
        indices = list(self.file_listbox.curselection())
        for i in reversed(indices):
            self.file_listbox.delete(i)
            del self.file_paths[i]
        self._refresh_count()

    def _clear_files(self):
        if self.is_reviewing:
            return
        self.file_paths.clear()
        self.file_listbox.delete(0, tk.END)
        self.result_box.delete("1.0", tk.END)
        self.all_results.clear()
        self.progress.set(0)
        self._refresh_count()
        self.status_label.configure(
            text="파일을 추가한 후 [검토 시작]을 눌러주세요."
        )

    def _refresh_count(self):
        count = len(self.file_paths)
        self.count_label.configure(
            text=f"파일 {count}개"
        )
        if count == 0:
            self._show_drop_hint()
        else:
            self._hide_drop_hint()

    @staticmethod
    def _format_listbox_item(filename: str) -> str:
        """파일 목록 표시용 텍스트에 좌우 여백을 적용한다."""
        return f"  {filename}  "

    def _update_db_source_label(self):
        if not hasattr(self, "db_source_label"):
            return
        count = len(self.master_names)
        if self.is_server_synced:
            if self.db_date:
                try:
                    date_text = datetime.strptime(self.db_date, "%Y-%m-%d").strftime("%y/%m/%d")
                except ValueError:
                    date_text = self.db_date
                text = f"DB서버 : {count}개(동기성공-{date_text}기점)"
            else:
                text = f"DB서버 : {count}개(동기성공)"
        else:
            text = f"DB서버 : {count}개(동기실패)"
        self.db_source_label.configure(text=text)

    def _build_drop_hint(self, parent):
        self.drop_hint_label = None
        self.drop_hint_image = None
        self.drop_hint_image_label = None

        image_path = resource_path("drag_guide.png")
        if Image is not None and os.path.exists(image_path):
            try:
                with Image.open(image_path) as src:
                    pil_image = src.convert("RGBA")

                # 투명 배경이 주변 UI와 어울리도록 흰색 배경에 합성한다.
                white_bg = Image.new("RGBA", pil_image.size, (255, 255, 255, 255))
                white_bg.paste(pil_image, (0, 0), pil_image)
                pil_image = white_bg

                width, height = pil_image.size
                target_width = 280
                target_height = max(1, int((height / width) * target_width))

                self.drop_hint_image = ctk.CTkImage(
                    light_image=pil_image,
                    dark_image=pil_image,
                    size=(target_width, target_height)
                )
                self.drop_hint_image_label = ctk.CTkLabel(
                    parent,
                    text="",
                    fg_color="transparent",
                    image=self.drop_hint_image
                )
                return
            except Exception:
                pass

        self.drop_hint_label = ctk.CTkLabel(
            parent,
            text="📂 파일을 여기에 드래그하거나\n[파일 추가] 버튼을 사용하세요",
            font=self.TITLE_FONT,
            text_color="#999999",
            justify="center"
        )

    def _show_drop_hint(self):
        if getattr(self, "drop_hint_image_label", None) is not None:
            self.drop_hint_image_label.place(relx=0.5, rely=0.5, anchor="center")
            return
        if getattr(self, "drop_hint_label", None) is not None:
            self.drop_hint_label.place(relx=0.5, rely=0.5, anchor="center")

    def _hide_drop_hint(self):
        if getattr(self, "drop_hint_image_label", None) is not None:
            self.drop_hint_image_label.place_forget()
        if getattr(self, "drop_hint_label", None) is not None:
            self.drop_hint_label.place_forget()

    @staticmethod
    def _decode_drop_path(item) -> str:
        if isinstance(item, bytes):
            for enc in ("utf-8", "cp949", "euc-kr", "mbcs"):
                try:
                    return item.decode(enc)
                except (UnicodeDecodeError, LookupError):
                    continue
            return ""
        return str(item)

    def _on_drop_files(self, file_list):
        """windnd 콜백 - 스레드 안전하게 큐에 적재"""
        try:
            self._drop_queue.put_nowait(list(file_list))
        except Exception as e:
            print(f"드래그 드롭 큐 적재 오류: {e}")

    def _poll_drop_queue(self):
        """메인 스레드에서 드롭 큐를 주기적으로 처리"""
        try:
            while True:
                dropped = self._drop_queue.get_nowait()
                self._process_dropped_files(dropped)
        except queue.Empty:
            pass
        except Exception as e:
            print(f"드래그 드롭 큐 처리 오류: {e}")
        finally:
            try:
                self.after(120, self._poll_drop_queue)
            except tk.TclError:
                # 종료 직후에는 after 재등록이 실패할 수 있음
                pass

    def _process_dropped_files(self, file_list):
        """메인 스레드에서 드롭된 파일 처리"""
        if self.is_reviewing:
            return

        try:
            for item in file_list:
                fp = self._decode_drop_path(item)
                if not fp:
                    continue

                fp = fp.strip().strip('"').strip("'")
                if not fp:
                    continue

                if fp.startswith("{") and fp.endswith("}"):
                    fp = fp[1:-1].strip()

                if not os.path.isfile(fp):
                    continue

                ext = os.path.splitext(fp)[1].lower()
                if ext not in SUPPORTED_EXTENSIONS:
                    continue

                if fp in self.file_paths:
                    continue

                self.file_paths.append(fp)
                self.file_listbox.insert(
                    tk.END, self._format_listbox_item(os.path.basename(fp))
                )

            self._refresh_count()
        except Exception as e:
            print(f"드래그 드롭 처리 오류: {e}")

    # ── UI 잠금 ──
    def _lock_ui(self):
        self.review_btn.configure(
            state="disabled", text="검토 중..."
        )
        if hasattr(self, "sync_db_btn"):
            self.sync_db_btn.configure(state="disabled")

    def _unlock_ui(self):
        self.review_btn.configure(
            state="normal", text="검토 시작"
        )
        if hasattr(self, "sync_db_btn"):
            state = "disabled" if self.is_syncing else "normal"
            self.sync_db_btn.configure(state=state)

    # ── 검토 ──
    def _start_review(self):
        if self.is_reviewing or self.is_syncing:
            return
        if not self.file_paths:
            messagebox.showwarning(
                "알림", "검토할 파일을 먼저 추가해주세요."
            )
            return

        self.is_reviewing = True
        self._lock_ui()
        self.result_box.delete("1.0", tk.END)
        self.all_results.clear()
        self.progress.set(0)

        snapshot = list(self.file_paths)
        thread = threading.Thread(
            target=self._run_review,
            args=(snapshot,),
            daemon=True
        )
        thread.start()

    def _run_review(self, files: list):
        try:
            total = len(files)
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            started_at = perf_counter()

            self._log("=" * 58)
            self._log(
                f"  검토 시작  |  {total}개 파일  |  {now}"
            )
            self._log("=" * 58)
            self._set_progress(0.0)

            grand_total = 0
            grand_match = 0
            grand_warning = 0
            grand_mismatch = 0

            def _parse_issue_and_official(
                issue: str, suggestion: str, is_warning: bool
            ) -> tuple[str, str]:
                """이슈 문구에서 사유와 공식명칭을 분리한다."""
                reason = (issue or "").strip()
                official = (suggestion or "").strip()

                if "특정불가" in reason:
                    lines = [line.strip() for line in reason.split("\n") if line.strip()]
                    if lines:
                        reason = lines[0]
                    if len(lines) > 1 and not official:
                        official = " / ".join(lines[1:])
                elif "→" in reason:
                    tail = ""
                    if " → " in reason:
                        reason, tail = reason.rsplit(" → ", 1)
                    else:
                        parts = [p.strip() for p in reason.split("→") if p.strip()]
                        if len(parts) > 1:
                            reason = "→".join(parts[:-1]).strip()
                            tail = parts[-1]
                        elif parts:
                            reason = parts[0]
                    tail = tail.strip()
                    if tail.startswith("공식:"):
                        official = tail[3:].strip()
                    elif tail.startswith("후보:"):
                        official = tail[3:].strip()
                    elif tail:
                        official = tail

                # 경고 섹션에서는 "경고:" 접두어를 제거해 중복 표시를 피한다.
                if is_warning and reason.startswith("경고:"):
                    reason = reason[len("경고:"):].strip()

                if not reason:
                    reason = "경고" if is_warning else "불일치"
                return reason, official

            def _log_section(title: str, items: list[dict], is_warning: bool):
                """검토 항목을 박스 스타일 섹션으로 출력한다."""
                if not items:
                    return

                bar = "─" * 50
                self._log("")
                self._log(f"  ■ {title} ({len(items)}건)")
                self._log(f"  ┌{bar}")

                for i, item in enumerate(items):
                    self._log(f"  │ {item.get('input', '')}")
                    location = item.get("location", "")
                    if location:
                        self._log(f"  │   위치: {location}")

                    reason, official = _parse_issue_and_official(
                        item.get("issue", ""),
                        item.get("suggestion", ""),
                        is_warning,
                    )
                    self._log(f"  │   사유: {reason}")
                    if official:
                        self._log(f"  │   공식: {official}")

                    connector = "└" if i == len(items) - 1 else "├"
                    self._log(f"  {connector}{bar}")

            for idx, fpath in enumerate(files):
                fname = os.path.basename(fpath)
                self._set_status(
                    f"검토 중... ({idx+1}/{total}) {fname}"
                )

                cb = lambda v, idx=idx: self._update_file_progress(idx, total, v)
                result = self.engine.review_file(fpath, progress_callback=cb)
                self.all_results.append(result)

                warning_count = int(result.get("warning", 0) or 0)
                if warning_count == 0 and result.get("details"):
                    warning_count = sum(
                        1 for d in result["details"]
                        if d.get("status") not in {"일치", "불일치"}
                    )

                grand_total += int(result.get("total", 0) or 0)
                grand_match += int(result.get("matched", 0) or 0)
                grand_warning += warning_count
                grand_mismatch += int(result.get("mismatched", 0) or 0)

                if result.get("error"):
                    self._log(f"\n[{idx+1}] {fname}  →  오류")
                    self._log(f"     {result['error']}")
                else:
                    warn_items = [
                        d for d in result.get("details", [])
                        if d.get("status") == "경고"
                    ]
                    ng_items = [
                        d for d in result.get("details", [])
                        if d.get("status") == "불일치"
                    ]

                    if not warn_items and not ng_items:
                        self._log(
                            f"\n[{idx+1}] {fname}  →  "
                            f"적합 (일치 {result.get('matched', 0)}개)"
                        )
                    else:
                        sep = "─" * 53
                        self._log(f"\n{sep}")
                        self._log(
                            f"[{idx+1}] {fname}  →  "
                            f"경고 {len(warn_items)}건 | 불일치 {len(ng_items)}건"
                        )
                        self._log(sep)

                        # 가독성을 위해 불일치를 먼저, 경고를 뒤에 출력한다.
                        _log_section("불일치", ng_items, is_warning=False)
                        _log_section("경고", warn_items, is_warning=True)

                        self._log(sep)

                # 파일 단위 완료 시 즉시 진행률을 반영한다.
                self._set_progress((idx + 1) / total if total else 0.0)

            # 요약
            elapsed = perf_counter() - started_at
            self._log("\n" + "=" * 58)
            self._log(f"  검토 완료!  |  소요시간: {elapsed:.1f}s")
            self._log(
                f"  발견 {grand_total}개  |  "
                f"일치 {grand_match}개  |  "
                f"경고 {grand_warning}개  |  "
                f"불일치 {grand_mismatch}개"
            )
            self._log("=" * 58)

            self._set_status(
                f"검토 완료  |  발견 {grand_total}개 : "
                f"일치 {grand_match}개 | 경고 {grand_warning}개 | "
                f"불일치 {grand_mismatch}개  |  {elapsed:.1f}s"
            )

        except Exception as e:
            self._log(f"\n  예기치 않은 오류: {e}")
            self._set_status(f"오류 발생: {e}")

        finally:
            self.is_reviewing = False
            self.after(0, self._unlock_ui)

    # ── 스레드 안전 UI ──
    def _log(self, text: str):
        self.after(0, lambda t=text: self._do_log(t))

    def _do_log(self, text: str):
        self.result_box.insert(tk.END, text + "\n")
        self.result_box.see(tk.END)

    def _set_status(self, text: str):
        self.after(
            0, lambda t=text: self.status_label.configure(text=t)
        )

    def _apply_icon_to_toplevel(self, toplevel):
        """Toplevel 윈도우에 앱 아이콘을 적용한다.

        CustomTkinter의 CTkToplevel은 iconbitmap 상속이 불안정하므로
        여러 방법을 순차적으로 시도한다.

        Args:
            toplevel: 아이콘을 적용할 CTkToplevel 인스턴스.
        """
        if not hasattr(self, '_icon_path') or not os.path.exists(self._icon_path):
            return

        icon_path = self._icon_path

        def _try_set_icon():
            try:
                toplevel.iconbitmap(icon_path)
            except Exception:
                pass
            try:
                toplevel.wm_iconbitmap(icon_path)
            except Exception:
                pass
            try:
                from PIL import Image, ImageTk
                icon_img = Image.open(icon_path)
                icon_photo = ImageTk.PhotoImage(icon_img)
                toplevel.iconphoto(False, icon_photo)
                toplevel._icon_ref = icon_photo  # GC 방지
            except Exception:
                pass

        # CTkToplevel 렌더링 완료 후 아이콘 적용을 시도한다.
        toplevel.after(100, _try_set_icon)
        toplevel.after(300, _try_set_icon)
        toplevel.after(600, _try_set_icon)

    def _set_progress(self, value: float):
        self.after(0, lambda v=value: self.progress.set(v))

    def _update_file_progress(self, file_idx: int, total_files: int, value: float):
        """파일 내부 진행률을 전체 진행률로 환산하여 프로그레스바를 갱신한다.

        Args:
            file_idx: 현재 처리 중인 파일 인덱스(0-based).
            total_files: 전체 파일 수.
            value: 현재 파일 내부 진행률(0.0~1.0).
        """
        if total_files <= 0:
            return

        # 엔진 콜백 값의 범위를 보정해 UI 진행률 오차를 방지한다.
        clamped = max(0.0, min(1.0, float(value)))
        overall = (file_idx + clamped) / total_files
        self._pending_progress = overall
        try:
            self.event_generate("<<UpdateProgress>>", when="tail")
        except Exception:
            pass

    def _on_update_progress(self, event=None):
        """메인 스레드에서 프로그레스바를 갱신한다."""
        try:
            self.progress.set(self._pending_progress)
        except Exception:
            pass

    # ── 리포트 ──
    def _sync_db(self):
        if self.is_reviewing or self.is_syncing:
            return
        self.is_syncing = True
        if hasattr(self, "sync_db_btn"):
            self.sync_db_btn.configure(state="disabled", text="동기중...")
        self._log("DB 동기화 시도 중...")
        self._set_status("DB 동기화 시도 중...")

        thread = threading.Thread(target=self._run_sync_db, daemon=True)
        thread.start()

    def _run_sync_db(self):
        try:
            names, source, db_date = load_master_names(MASTER_DB_URL)
            if source != "서버":
                error = get_last_master_load_error() or "서버 DB 동기화 실패"
                raise RuntimeError(error)
            self._sync_queue.put(("success", (names, db_date)))
        except Exception as e:
            self._sync_queue.put(("failure", str(e)))

    def _poll_sync_queue(self):
        try:
            while True:
                event_type, payload = self._sync_queue.get_nowait()
                if event_type == "success":
                    self._apply_synced_master(payload)
                elif event_type == "failure":
                    self._apply_sync_failure(payload)
        except queue.Empty:
            pass
        except Exception as e:
            print(f"동기화 큐 처리 오류: {e}")
        finally:
            try:
                self.after(120, self._poll_sync_queue)
            except tk.TclError:
                pass

    def _apply_synced_master(self, payload):
        names, db_date = payload
        self.master_names = list(dict.fromkeys(names))
        self.matcher = NameMatcher(self.master_names)
        self.engine = ReviewEngine(self.matcher)
        self.db_source = "서버"
        self.db_date = db_date
        self.is_server_synced = True
        self._update_db_source_label()

        self._log(f"동기화 완료 ({len(self.master_names)}개 명칭 로드)")
        self._set_status(f"DB 동기화 완료 ({len(self.master_names)}개)")
        self.is_syncing = False
        if hasattr(self, "sync_db_btn"):
            self.sync_db_btn.configure(state="normal", text="DB동기")

    def _apply_sync_failure(self, error_message: str):
        self._log(
            f"동기화 실패: {error_message}, "
            f"기존 DB 유지 ({len(self.master_names)}개)"
        )
        self._set_status("DB 동기화 실패 (기존 DB 유지)")
        self.db_source = "내장"
        self.db_date = None
        self.is_server_synced = False
        self._update_db_source_label()
        self.is_syncing = False
        if hasattr(self, "sync_db_btn"):
            self.sync_db_btn.configure(state="normal", text="DB동기")

    def _show_help(self):
        """사용설명서 팝업을 표시한다."""
        popup = ctk.CTkToplevel(self)
        popup.title("공사명칭 검토기 — 사용설명서")
        popup.geometry("720x620")
        popup.grab_set()
        self._apply_icon_to_toplevel(popup)

        textbox = ctk.CTkTextbox(
            popup,
            font=self.DEFAULT_FONT,
            wrap="word",
        )
        textbox.pack(fill="both", expand=True, padx=15, pady=15)

        help_text = """\
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  공사명칭 검토기 사용설명서  (v1.0)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. 개요
  보고서 파일(PDF, HWP, HWPX, XLSX, DOCX, CSV)에서
  추출한 공사명칭을 마스터 DB와 비교하여
  일치 / 불일치를 자동 판정하는 도구입니다.


2. 파일 추가
  • [파일 추가] : 개별 파일을 선택하여 추가합니다.
  • [폴더 추가] : 폴더 내 지원 파일을 일괄 추가합니다.
  • 드래그 앤 드롭 : 파일을 좌측 목록에 직접 끌어다 놓을 수 있습니다.
  • [선택 삭제] / [전체 삭제] : 목록에서 파일을 제거합니다.


3. 검토 실행
  파일을 추가한 뒤 [검토 시작] 버튼을 클릭하면
  각 파일에서 공사명칭을 추출하여 마스터 DB와 비교합니다.

  결과는 우측 패널에 표시되며,
  불일치 항목에는 사유와 공식 명칭 후보가 함께 안내됩니다.


4. 스냅샷 보기 (PDF 전용)
  검토 완료 후 [스냅샷 보기] 버튼을 클릭하면
  불일치가 발견된 PDF 페이지를 하이라이트하여
  이미지로 확인할 수 있습니다.

  팝업에서 [전체 PNG 저장]으로 일괄 저장도 가능합니다.


5. 리포트 저장
  [리포트 저장 (Excel)] 버튼으로
  검토 결과를 Excel 파일로 내보낼 수 있습니다.


6. DB 동기화
  좌측 하단 [DB동기] 버튼으로 서버의 최신 마스터 DB를
  수동 동기화할 수 있습니다.
  프로그램 시작 시에도 자동으로 서버 동기화를 시도합니다.
  동기화 실패 시 내장 DB로 검토가 진행됩니다.


7. 지원 파일 형식
  HWP, HWPX, PDF, XLSX, DOCX, CSV


8. 판정 기준
  • 일치 : 마스터 DB에 동일 명칭이 존재
  • 경고 : 접두어 차이 또는 유사명칭 감지 (유사도 기반)
  • 불일치 : 마스터 DB에 해당 명칭 없음


9. 문의처(DB 갱신 및 오류)
  • 전산파트 02-6984-9090
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""

        textbox.insert("1.0", help_text)
        textbox.configure(state="disabled")

        ctk.CTkButton(
            popup,
            text="닫기",
            width=100,
            height=34,
            command=popup.destroy,
        ).pack(pady=(0, 15))

    def _show_snapshots(self):
        """불일치가 있는 PDF 페이지를 하이라이트 스냅샷으로 팝업 표시한다."""
        if self.is_reviewing:
            return
        if not self.all_results:
            messagebox.showwarning(
                "스냅샷", "검토 결과가 없습니다.\n먼저 검토를 실행해주세요."
            )
            return

        if Image is None:
            messagebox.showwarning(
                "스냅샷", "Pillow가 설치되지 않아 스냅샷을 표시할 수 없습니다."
            )
            return

        # PDF 파일별 불일치 항목을 수집해 페이지 스냅샷을 생성한다.
        pdf_snapshots: list[tuple[str, int, bytes]] = []
        for result in self.all_results:
            filepath = result.get("path", "")
            if not filepath or not filepath.lower().endswith(".pdf"):
                continue

            ng_items = [
                d for d in result.get("details", [])
                if d["status"] == "불일치"
            ]
            if not ng_items:
                continue

            snapshots = generate_highlight_snapshots(filepath, ng_items)
            fname = os.path.basename(filepath)
            for page_num, png_bytes in snapshots:
                pdf_snapshots.append((fname, page_num, png_bytes))

        if not pdf_snapshots:
            messagebox.showinfo(
                "스냅샷",
                "표시할 PDF 스냅샷이 없습니다.\n(PDF 파일에 불일치가 없거나, PDF가 아닌 파일만 검토됨)"
            )
            return

        popup = ctk.CTkToplevel(self)
        popup.title("불일치 스냅샷 보기")
        popup.geometry("900x700")
        popup.grab_set()
        self._apply_icon_to_toplevel(popup)

        info_frame = ctk.CTkFrame(popup, fg_color="transparent")
        info_frame.pack(fill="x", padx=10, pady=(10, 5))

        ctk.CTkLabel(
            info_frame,
            text=f"불일치 페이지 {len(pdf_snapshots)}개",
            font=self.BOLD_FONT
        ).pack(side="left")

        def _save_all_snapshots():
            """현재 팝업의 스냅샷을 PNG로 일괄 저장한다."""
            folder = filedialog.askdirectory(title="스냅샷 저장 폴더 선택")
            if not folder:
                return

            saved = 0
            for fname, page_num, png_bytes in pdf_snapshots:
                base = os.path.splitext(fname)[0]
                out_path = os.path.join(folder, f"검토_{base}_P{page_num}.png")
                with open(out_path, "wb") as f:
                    f.write(png_bytes)
                saved += 1
            messagebox.showinfo("저장 완료", f"{saved}개 스냅샷 저장됨\n{folder}")

        ctk.CTkButton(
            info_frame, text="전체 PNG 저장",
            width=130, height=32,
            fg_color="#28A745", hover_color="#218838",
            command=_save_all_snapshots
        ).pack(side="right")

        scroll_frame = ctk.CTkScrollableFrame(popup)
        scroll_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        # 이미지 객체 참조를 유지해 가비지 컬렉션으로 사라지지 않게 한다.
        popup._snapshot_images = []

        for fname, page_num, png_bytes in pdf_snapshots:
            ctk.CTkLabel(
                scroll_frame,
                text=f"📄 {fname} — P{page_num}",
                font=self.BOLD_FONT
            ).pack(pady=(12, 4), anchor="w")

            pil_image = Image.open(io.BytesIO(png_bytes))
            max_width = 860
            if pil_image.width > max_width:
                ratio = max_width / pil_image.width
                new_size = (max_width, int(pil_image.height * ratio))
                pil_image = pil_image.resize(new_size, Image.LANCZOS)

            ctk_image = ctk.CTkImage(
                light_image=pil_image,
                dark_image=pil_image,
                size=(pil_image.width, pil_image.height)
            )
            popup._snapshot_images.append(ctk_image)

            img_label = ctk.CTkLabel(
                scroll_frame,
                image=ctk_image,
                text=""
            )
            img_label.pack(pady=(0, 8))

    def _save_report(self):
        if self.is_reviewing:
            return

        if not self.all_results:
            messagebox.showwarning(
                "알림", "먼저 검토를 실행해주세요."
            )
            return

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        path = filedialog.asksaveasfilename(
            title="리포트 저장",
            defaultextension=".xlsx",
            initialfile=f"불일치검토리포트_{ts}.xlsx",
            filetypes=[("Excel", "*.xlsx")]
        )
        if not path:
            return

        try:
            save_excel_report(
                self.all_results, path, self.master_names
            )
            messagebox.showinfo(
                "완료",
                f"리포트가 저장되었습니다.\n\n{path}"
            )
            os.startfile(path)
        except Exception as e:
            messagebox.showerror("오류", f"저장 실패:\n{e}")


if __name__ == "__main__":
    app = App()
    app.mainloop()

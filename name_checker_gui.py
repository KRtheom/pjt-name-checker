"""
공사현장 명칭 일원화 검토 프로그램 v3.0 (GUI)
"""

import os
import threading
from datetime import datetime

import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox

from engine import (
    SUPPORTED_EXTENSIONS,
    DEFAULT_MASTER_NAMES,
    NameMatcher,
    ReviewEngine,
    save_excel_report,
)


class App(ctk.CTk):

    def __init__(self):
        super().__init__()

        self.title("공사현장 명칭 일원화 검토 프로그램 v3.0")
        self.geometry("1150x780")
        self.minsize(950, 650)

        ctk.set_appearance_mode("light")
        ctk.set_default_color_theme("blue")

        self.master_names = list(DEFAULT_MASTER_NAMES)
        self.matcher = NameMatcher(self.master_names)
        self.engine = ReviewEngine(self.matcher)

        self.file_paths = []
        self.all_results = []
        self.is_reviewing = False

        self._build_ui()

    def _build_ui(self):

        # 상단
        top = ctk.CTkFrame(self, fg_color="#2F5496",
                           corner_radius=0, height=55)
        top.pack(fill="x")
        top.pack_propagate(False)

        ctk.CTkLabel(
            top, text="  공사현장 명칭 일원화 검토",
            font=ctk.CTkFont(size=20, weight="bold"),
            text_color="white"
        ).pack(side="left", padx=15, pady=12)

        ctk.CTkLabel(
            top, text="v3.0  |  HWP · PDF · XLSX · DOCX  ",
            font=ctk.CTkFont(size=12), text_color="#B0C4DE"
        ).pack(side="right", padx=15)

        # 메인
        main = ctk.CTkFrame(self, fg_color="transparent")
        main.pack(fill="both", expand=True, padx=12, pady=8)

        # 좌측
        left = ctk.CTkFrame(main, width=380)
        left.pack(side="left", fill="both", padx=(0, 6))
        left.pack_propagate(False)

        ctk.CTkLabel(
            left, text="검토 대상 파일",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(pady=(10, 6), padx=10, anchor="w")

        bf = ctk.CTkFrame(left, fg_color="transparent")
        bf.pack(fill="x", padx=10)

        ctk.CTkButton(bf, text="파일 추가", width=105,
                       command=self._add_files
                       ).pack(side="left", padx=(0, 4))
        ctk.CTkButton(bf, text="폴더 추가", width=105,
                       command=self._add_folder
                       ).pack(side="left", padx=(0, 4))
        ctk.CTkButton(bf, text="선택 삭제", width=90,
                       fg_color="#6C757D", hover_color="#5A6268",
                       command=self._remove_selected
                       ).pack(side="left", padx=(0, 4))
        ctk.CTkButton(bf, text="전체 삭제", width=80,
                       fg_color="#DC3545", hover_color="#C82333",
                       command=self._clear_files
                       ).pack(side="left")

        lf = ctk.CTkFrame(left, fg_color="transparent")
        lf.pack(fill="both", expand=True, padx=10, pady=8)

        sb = tk.Scrollbar(lf)
        sb.pack(side="right", fill="y")

        self.file_listbox = tk.Listbox(
            lf, font=("맑은 고딕", 10),
            selectmode=tk.EXTENDED, activestyle="none",
            yscrollcommand=sb.set
        )
        self.file_listbox.pack(fill="both", expand=True)
        sb.config(command=self.file_listbox.yview)

        self.count_label = ctk.CTkLabel(
            left, text="파일 0개", font=ctk.CTkFont(size=11)
        )
        self.count_label.pack(padx=10, pady=(0, 10), anchor="w")

        # 우측
        right = ctk.CTkFrame(main)
        right.pack(side="right", fill="both", expand=True)

        ctk.CTkLabel(
            right, text="불일치 검토 결과",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(pady=(10, 6), padx=10, anchor="w")

        self.result_box = ctk.CTkTextbox(
            right,
            font=ctk.CTkFont(family="Consolas", size=11),
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

        self.status_label = ctk.CTkLabel(
            bottom,
            text="파일을 추가한 후 [검토 시작]을 눌러주세요.",
            font=ctk.CTkFont(size=11)
        )
        self.status_label.pack(side="left")

        ctk.CTkButton(
            bottom, text="리포트 저장 (Excel)",
            width=170, height=38,
            fg_color="#28A745", hover_color="#218838",
            font=ctk.CTkFont(size=13, weight="bold"),
            command=self._save_report
        ).pack(side="right", padx=(8, 0))

        self.review_btn = ctk.CTkButton(
            bottom, text="검토 시작",
            width=140, height=38,
            font=ctk.CTkFont(size=13, weight="bold"),
            command=self._start_review
        )
        self.review_btn.pack(side="right")

    # ── 파일 관리 ──
    def _add_files(self):
        if self.is_reviewing:
            return
        types = [
            ("지원 형식", "*.hwp *.pdf *.xlsx *.docx"),
            ("HWP", "*.hwp"), ("PDF", "*.pdf"),
            ("Excel", "*.xlsx"), ("Word", "*.docx"),
        ]
        paths = filedialog.askopenfilenames(
            title="파일 선택", filetypes=types
        )
        for p in paths:
            if p not in self.file_paths:
                self.file_paths.append(p)
                self.file_listbox.insert(
                    tk.END, os.path.basename(p)
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
                        self.file_listbox.insert(tk.END, f)
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
        self.count_label.configure(
            text=f"파일 {len(self.file_paths)}개"
        )

    # ── UI 잠금 ──
    def _lock_ui(self):
        self.review_btn.configure(
            state="disabled", text="검토 중..."
        )

    def _unlock_ui(self):
        self.review_btn.configure(
            state="normal", text="검토 시작"
        )

    # ── 검토 ──
    def _start_review(self):
        if self.is_reviewing:
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

            self._log("=" * 58)
            self._log(
                f"  검토 시작  |  {total}개 파일  |  {now}"
            )
            self._log("=" * 58)

            grand_total = 0
            grand_match = 0
            grand_mismatch = 0

            for idx, fpath in enumerate(files):
                fname = os.path.basename(fpath)
                self._set_status(
                    f"검토 중... ({idx+1}/{total}) {fname}"
                )

                result = self.engine.review_file(fpath)
                self.all_results.append(result)

                grand_total += result["total"]
                grand_match += result["matched"]
                grand_mismatch += result["mismatched"]

                if result.get("error"):
                    self._log(f"\n[{idx+1}] {fname}  →  오류")
                    self._log(f"     {result['error']}")
                    continue

                # 불일치 항목만 추출
                ng_items = [
                    d for d in result["details"]
                    if d["status"] == "불일치"
                ]

                if not ng_items:
                    # 적합한 파일은 간단히 한 줄만
                    self._log(
                        f"\n[{idx+1}] {fname}  →  "
                        f"적합 (일치 {result['matched']}개)"
                    )
                else:
                    self._log(
                        f"\n[{idx+1}] {fname}  →  "
                        f"【불일치 {len(ng_items)}건 발견】"
                    )
                    for d in ng_items:
                        self._log(
                            f"     [ NG ] {d['input']}"
                        )
                        if d["issue"]:
                            for issue_line in d["issue"].split("\n"):
                                self._log(
                                    f"            → {issue_line}"
                                )

                self._set_progress((idx + 1) / total)

            # 요약
            self._log("\n" + "=" * 58)
            self._log("  검토 완료!")
            self._log(
                f"  발견 {grand_total}개  |  "
                f"일치 {grand_match}개 (참고)  |  "
                f"불일치 {grand_mismatch}개"
            )
            self._log("=" * 58)

            self._set_status(
                f"검토 완료  |  발견 {grand_total}개 : "
                f"불일치 {grand_mismatch}개"
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

    def _set_progress(self, value: float):
        self.after(0, lambda v=value: self.progress.set(v))

    # ── 리포트 ──
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

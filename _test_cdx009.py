from multiprocessing import freeze_support

import engine


def main():
    filepath = r"dist\260309 경영회의(유통실적 제외).pdf"
    text_items = engine.extract_text_from_file(filepath)
    names, src, db_date = engine.load_master_names()
    matcher = engine.NameMatcher(names)
    reviewer = engine.ReviewEngine(matcher)
    result = reviewer.review_file(filepath)

    print(f"master_source={src}, db_date={db_date}")
    print(f"text_items={len(text_items)}")
    print(
        f"total={result['total']}, matched={result['matched']}, "
        f"mismatched={result['mismatched']}, warning={result['warning']}"
    )

    for d in result["details"]:
        if d["status"] != "일치":
            inp = d.get("input", "")
            loc = d.get("location", "")
            issue = d.get("issue", "")
            sugg = d.get("suggestion", "")
            print(f"  [{loc}] {inp}")
            print(f"    사유: {issue}")
            print(f"    후보: {sugg}")
            print()

    ng_items = [d for d in result["details"] if d["status"] != "일치"]
    if ng_items:
        snapshots = engine.generate_highlight_snapshots(filepath, ng_items)
        for page_num, png_bytes in snapshots:
            out_path = f"_snapshot_P{page_num}.png"
            with open(out_path, "wb") as f:
                f.write(png_bytes)
            print(f"snapshot saved: {out_path} ({len(png_bytes)} bytes)")


if __name__ == "__main__":
    freeze_support()
    main()

from pathlib import Path


def main():
    import argparse
    import logging

    parser = argparse.ArgumentParser()
    parser.add_argument("epub_path", type=Path)
    args = parser.parse_args()
    epub_path = args.epub_path.expanduser()
    if not epub_path.exists():
        logging.error("EPUB file path doesn't exist")
        return 1
    create_kpf(epub_path)


def create_kpf(epub_path: Path):
    import shutil
    import tempfile

    from .epub import extract_epub
    from .kdf import create_kdf

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        kpf_dir = tmp_path / epub_path.stem
        kpf_dir.mkdir()
        resources_dir = kpf_dir / "resources"
        resources_dir.mkdir()
        shutil.copyfile(epub_path, kpf_dir / "book.epub")
        extract_epub(epub_path, tmp_path)
        create_kcb(kpf_dir)
        create_kdf(tmp_path, resources_dir / "book.kdf")
        shutil.make_archive(str(epub_path.with_suffix("")), "zip", kpf_dir)
        kpf_path = epub_path.with_suffix(".zip")
        kpf_path.rename(kpf_path.with_suffix(".kpf"))


def create_kcb(kpf_dir: Path):
    import json
    from importlib.metadata import version

    kpfgen_version = version("kpfgen")
    with (kpf_dir / "book.kcb").open("w") as f:
        data = {
            "metadata": {
                "book_path": "resources",
                "edited_tool_versions": [kpfgen_version],
                "source_path": "book.epub",
                "tool_name": "kpfgen",
                "tool_version": kpfgen_version,
            }
        }
        json.dump(data, f, indent=2, ensure_ascii=False)

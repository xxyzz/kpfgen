from dataclasses import dataclass, field
from pathlib import Path


def extract_epub(epub_path: Path, dest_path: Path) -> None:
    import zipfile

    with zipfile.ZipFile(epub_path) as zf:
        zf.extractall(dest_path)


@dataclass
class EPUBMetadata:
    language: str = ""
    title: str = ""
    description: str = ""
    author: str = ""
    publisher: str = ""
    cover_path: Path | None = None
    spine_paths: list[Path] = field(default_factory=list)
    toc: Path | None = None


NAMESPACES = {
    "n": "urn:oasis:names:tc:opendocument:xmlns:container",
    "opf": "http://www.idpf.org/2007/opf",
    "ops": "http://www.idpf.org/2007/ops",
    "xml": "http://www.w3.org/1999/xhtml",
    "dc": "http://purl.org/dc/elements/1.1/",
}


def get_epub_metadata(epub_dir: Path) -> EPUBMetadata:
    from urllib.parse import unquote

    from lxml import etree

    container_root = etree.parse(epub_dir / "META-INF" / "container.xml")
    opf_path_str = unquote(
        container_root.find(".//n:rootfile", NAMESPACES).get("full-path")
    )
    opf_path = epub_dir / opf_path_str
    if not opf_path.exists():
        opf_path = next(epub_dir.rglob(opf_path_str))
    opf_root = etree.parse(opf_path)
    metadata = EPUBMetadata()
    for element_type in ("language", "title", "description", "publisher"):
        for element in opf_root.iterfind(
            f"opf:metadata/dc:{element_type}", namespaces=NAMESPACES
        ):
            setattr(metadata, element_type, element.text)
    author_element = opf_root.find("opf:metadata/dc:creator", NAMESPACES)
    if author_element is not None:
        metadata.author = author_element.text
    cover_element = opf_root.find('opf:metadata/opf:meta[@name="cover"]', NAMESPACES)
    if cover_element is not None:
        cover_id = cover_element.get("content")
        metadata.cover_path = opf_path.parent / opf_root.find(
            f'opf:manifest/opf:item[@id="{cover_id}"]', NAMESPACES
        ).get("href")
    get_epub_spine(opf_root, metadata, opf_path)
    toc_element = opf_root.find('opf:manifest/opf:item[@properties="nav"]', NAMESPACES)
    if toc_element is not None:
        metadata.toc = opf_path.parent / toc_element.get("href")

    return metadata


def get_epub_spine(opf_root, metadata: EPUBMetadata, opf_path: Path) -> None:
    for spine_item in opf_root.iterfind("opf:spine/opf:itemref", NAMESPACES):
        manifest_item_id = spine_item.get("idref", "")
        manifest_item = opf_root.find(
            f'opf:manifest/opf:item[@id="{manifest_item_id}"]', NAMESPACES
        )
        if manifest_item is not None:
            metadata.spine_paths.append(opf_path.parent / manifest_item.get("href"))

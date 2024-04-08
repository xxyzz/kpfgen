from pathlib import Path
from typing import Any

from amazon.ion.core import IonType
from amazon.ion.simple_types import IonPyDict, IonPyList, IonPySymbol, IonPyText
from lxml import etree
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement


class KDF:
    def __init__(self) -> None:
        self.create_symbol_catalog()
        self.fragment_id = 0
        self.webdriver = init_webdriver()

    def create_symbol_catalog(self) -> None:
        from amazon.ion.symbols import SymbolTableCatalog, shared_symbol_table

        from .yj_symbols import YJ_CONVERSION_SYMBOLS, YJ_SYMBOLS

        self.symbol_table = shared_symbol_table(
            "YJ_symbols", 10, YJ_SYMBOLS + YJ_CONVERSION_SYMBOLS
        )
        self.catalog = SymbolTableCatalog()
        self.catalog.register(self.symbol_table)

    def create_kdf(self, tmp_dir: Path, db_path: Path) -> None:
        from .epub import get_epub_metadata

        self.res_dir = db_path.parent / "res"
        self.res_dir.mkdir(exist_ok=True)
        db_path.unlink(True)
        self.create_kdf_tables(db_path)
        self.insert_ion_symbol_table()
        self.epub_metadata = get_epub_metadata(tmp_dir)
        cover_res_id = self.insert_cover_section()
        self.insert_book_metadata(cover_res_id)
        section_ids = self.process_spine_items()
        self.create_document_data(section_ids)
        self.webdriver.quit()
        self.conn.commit()
        self.conn.close()

    def create_kdf_tables(self, db_path: Path) -> None:
        import sqlite3

        self.conn = sqlite3.connect(db_path)
        self.conn.executescript(
            """
            CREATE TABLE capabilities(
                key char(20), version smallint, primary key (key, version)
            ) without rowid;

            CREATE TABLE fragments(
                id char(40), payload_type char(10), payload_value blob, primary key (id)
            );

            CREATE TABLE fragment_properties(
                id char(40), key char(40), value char(40), primary key (id, key, value)
            ) without rowid;

            CREATE TABLE gc_fragment_properties(
                id varchar(40), key varchar(40), value varchar(40),
                primary key (id, key, value)
            ) without rowid;

            CREATE TABLE gc_reachable(id varchar(40), primary key (id)) without rowid;

            INSERT INTO capabilities VALUES('db.schema', 1);
            """
        )

    def insert_ion_symbol_table(self) -> None:
        from amazon.ion import simpleion

        from .yj_symbols import YJ_CONVERSION_SYMBOLS, YJ_SYMBOLS

        max_id = 9 + len(YJ_SYMBOLS) + len(YJ_CONVERSION_SYMBOLS)
        conversion_symbols = ", ".join(map(lambda x: f'"{x}"', YJ_CONVERSION_SYMBOLS))
        ion_str = f"""{{
          max_id: {max_id},
          imports: [{{name: "YJ_symbols", version: 10, max_id: {len(YJ_SYMBOLS)}}}],
          symbols: [{conversion_symbols}],
        }}
        """
        self.insert_blob_fragment("$ion_symbol_table", ion_str, "$ion_symbol_table")
        self.insert_fragment("max_id", "blob", simpleion.dumps(max_id, binary=True))
        self.insert_fragment_properties(
            [
                ("$ion_symbol_table", "element_type", "$ion_symbol_table"),
                ("max_id", "element_type", "max_id"),
            ]
        )

    def insert_blob_fragment(
        self, fragment_id: str, ion: str | IonPyDict, annotation: str = ""
    ) -> None:
        from amazon.ion import simpleion
        from amazon.ion.core import IonType

        if isinstance(ion, str):
            value = simpleion.loads(ion, catalog=self.catalog)
            value = IonPyDict.from_value(IonType.STRUCT, value, (annotation,))
        else:
            value = ion
        self.insert_fragment(
            fragment_id,
            "blob",
            remove_ion_table(
                simpleion.dumps(value, binary=True, imports=(self.symbol_table,))
            ),
        )

    def insert_book_metadata(self, cover_res_id: str) -> None:
        import random
        import string
        from importlib.metadata import version

        metadata = [
            {
                "key": "book_id",
                "value": "".join(
                    random.choices(string.digits + string.ascii_letters, k=23)
                ),
            }
        ]
        if len(cover_res_id) > 0:
            metadata.append({"key": "cover_img", "value": cover_res_id})
        for metadata_key in ("language", "title", "description", "author", "publisher"):
            value = getattr(self.epub_metadata, metadata_key)
            if len(value) > 0:
                metadata.append({"key": metadata_key, "value": value})

        ion = IonPyDict.from_value(
            IonType.STRUCT,
            {
                "categorised_metadata": [
                    {
                        "category": "kindle_ebook_metadata",
                        "metadata": [
                            {"key": "selection", "value": "enabled"},
                            {"key": "nested_span", "value": "enabled"},
                        ],
                    },
                    {
                        "category": "kindle_audit_metadata",
                        "metadata": [
                            {"key": "file_creator", "value": "kpfgen"},
                            {"key": "creator_version", "value": version("kpfgen")},
                        ],
                    },
                    {"category": "kindle_title_metadata", "metadata": metadata},
                ]
            },
            ("book_metadata",),
        )
        self.insert_blob_fragment("book_metadata", ion)
        self.insert_content_features()

    def insert_content_features(self) -> None:
        ion_text = """{
  kfx_id: content_features,
  features: [
    {
      namespace: "com.amazon.yjconversion",
      key: "reflow-style",
      version_info: {version: {major_version: 1, minor_version: 0}}
    }
  ]
}"""
        self.insert_blob_fragment("content_features", ion_text, "content_features")
        self.insert_fragment_property(
            "content_features", "element_type", "content_features"
        )

    def insert_cover_section(self) -> str:
        from PIL import Image

        if self.epub_metadata.cover_path is None:
            return ""
        with Image.open(self.epub_metadata.cover_path) as im:
            im_width, im_height = im.size

        section_id = self.create_fragment_id("c")
        section_struct_id = self.create_fragment_id("i")
        story_id = self.create_fragment_id("l")
        section_text = f"""{{
  section_name: kfx_id::"{section_id}",
  page_templates: [
    structure::{{
      kfx_id: kfx_id::"{section_struct_id}",
      story_name: kfx_id::"{story_id}",
      fixed_width: {im_width},
      fixed_height: {im_height},
      layout: scale_fit,
      float: center,
      type: container
    }}
  ]
}}"""
        self.insert_blob_fragment(section_id, section_text, "section")
        self.insert_fragment_property(section_id, "element_type", "section")

        struct_id = self.create_fragment_id("i")
        storyline_text = f"""{{
  story_name: kfx_id::"{story_id}",
  content_list: [
    kfx_id::"{struct_id}"
  ]
}}"""
        self.insert_blob_fragment(story_id, storyline_text, "storyline")
        self.insert_fragment_properties(
            [
                (story_id, "child", struct_id),
                (story_id, "child", story_id),
                (story_id, "element_type", "storyline"),
            ]
        )

        res_id = self.insert_image_resource(self.epub_metadata.cover_path)
        style_id = self.create_fragment_id("s")
        style_text = f"""{{
  font_size: {{value: 1.0e0, unit: rem}},
  line_height: {{value: 1.0e0, unit: lh}},
  style_name: kfx_id::"{style_id}"
}}"""
        self.insert_blob_fragment(style_id, style_text, "style")

        struct_text = f"""{{
  kfx_id: kfx_id::"{struct_id}",
  style: kfx_id::"{style_id}",
  type: image,
  resource_name: kfx_id::"{res_id}"
}}"""
        self.insert_blob_fragment(struct_id, struct_text, "structure")
        self.insert_fragment_property(struct_id, "element_type", "structure")
        self.insert_fragment_property(struct_id, "child", res_id)

        spm_text = f"""{{
  section_name: kfx_id::"{section_id}",
  contains: [[1, kfx_id::"{section_struct_id}"], [2, kfx_id::"{struct_id}"]]
}}"""
        spm_id = f"{section_id}-spm"
        self.insert_blob_fragment(spm_id, spm_text, "section_position_id_map")
        self.insert_fragment_property(spm_id, "element_type", "section_position_id_map")
        self.insert_section_auxiliary_data(section_id)
        return res_id

    def create_fragment_id(self, prefix: str) -> str:
        fragment_id_str = prefix + int_to_base32(self.fragment_id)
        self.fragment_id += 1
        return fragment_id_str

    def insert_image_resource(self, image_path: Path) -> str:
        import shutil

        from PIL import Image

        with Image.open(image_path) as im:
            im_width, im_height = im.size
            if im.mode == "RGBA":  # convert to JPEG, can't display PNG
                # only use `im.convert(RGB)` somehow creates all black image
                new_im = Image.new("RGBA", im.size, (255, 255, 255))
                new_im.paste(im, mask=im.split()[3])
                new_im = new_im.convert("RGB")
                image_path = image_path.with_suffix(".jpg")
                new_im.save(image_path)

        res_id = self.create_fragment_id("e")
        res_loc_id = self.create_fragment_id("rsrc")
        res_text = f"""{{
  format: jpg,
  location: "{res_loc_id}",
  resource_width: {im_width},
  resource_name: kfx_id::"{res_id}",
  resource_height: {im_height}
}}"""
        self.insert_blob_fragment(res_id, res_text, "external_resource")
        self.insert_fragment_properties(
            [
                (res_id, "child", res_loc_id),
                (res_id, "element_type", "external_resource"),
            ]
        )
        shutil.copy(image_path, self.res_dir / res_loc_id)
        self.insert_fragment(res_loc_id, "path", f"res/{res_loc_id}")
        self.insert_fragment_property(res_loc_id, "element_type", "bcRawMedia")
        return res_id

    def insert_fragment(
        self, fragment_id: str, payload_type: str, payload_value: str | bytes
    ) -> None:
        self.conn.execute(
            "INSERT INTO fragments VALUES(?, ?, ?)",
            (fragment_id, payload_type, payload_value),
        )

    def insert_fragment_property(self, fragment_id: str, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO fragment_properties VALUES(?, ?, ?)",
            (fragment_id, key, value),
        )

    def insert_fragment_properties(self, data) -> None:
        self.conn.executemany("INSERT INTO fragment_properties VALUES(?, ?, ?)", data)

    def insert_section_auxiliary_data(self, section_id: str) -> None:
        ad_id = section_id + "-ad"
        ion_text = f"""{{
  kfx_id: kfx_id::"{ad_id}",
  metadata: [{{key: "IS_TARGET_SECTION", value: true}}]
}}"""
        self.insert_blob_fragment(ad_id, ion_text, "auxiliary_data")
        self.insert_fragment_properties(
            [
                (section_id, "child", ad_id),
                (ad_id, "element_type", "auxiliary_data"),
            ]
        )

    def process_spine_items(self) -> list[str]:
        first_structure_ids: dict[str, str] = {}
        section_ids = ["c0"]
        all_structure_ids = {}
        for xml_path in self.epub_metadata.spine_paths:
            section_id, structure_ids = self.create_section(xml_path)
            section_ids.append(section_id)
            if len(structure_ids) > 0:
                first_structure_ids[xml_path.name] = structure_ids[0][0]
                all_structure_ids[section_id] = structure_ids
        self.create_book_navigation(first_structure_ids)
        self.create_section_pid_count_map(all_structure_ids)
        self.create_location_map(all_structure_ids)
        return section_ids

    def create_section(self, xml_path: Path) -> tuple[str, list[tuple[str, int]]]:
        section_id = self.create_fragment_id("c")
        section_struct_id = self.create_fragment_id("i")
        storyline_id = self.create_fragment_id("l")
        section_ion = f"""{{
  section_name: kfx_id::"{section_id}",
  page_templates: [
    structure::{{
      kfx_id: kfx_id::"{section_struct_id}",
      story_name: kfx_id::"{storyline_id}",
      type: text
    }}
  ]
}}"""
        self.insert_blob_fragment(section_id, section_ion, "section")
        self.insert_section_auxiliary_data(section_id)
        spm_list = self.create_storyline(xml_path, storyline_id)
        self.insert_fragment_properties(
            [
                (section_id, "element_type", "section"),
                (section_id, "child", storyline_id),
            ]
        )
        self.create_section_spm(section_id, section_struct_id, spm_list)
        return section_id, spm_list

    def create_section_spm(
        self, section_id: str, section_struct_id: str, spm_list: list[tuple[str, int]]
    ) -> None:
        spm_id = f"{section_id}-spm"
        new_spm_list = []
        location = 1
        for structure_id, content_len in spm_list:
            new_spm_list.append((structure_id, location))
            location += content_len
        spm_contains = ", ".join(
            f'[{loc}, kfx_id::"{s_id}"]' for (s_id, loc) in new_spm_list
        )
        spm_ion = f"""{{
  section_name: kfx_id::"{section_id}",
  contains: [[1, kfx_id::"{section_struct_id}"], {spm_contains}]
}}"""
        self.insert_blob_fragment(spm_id, spm_ion, "section_position_id_map")
        self.insert_fragment_property(spm_id, "element_type", "section_position_id_map")

    def create_storyline(self, xml_path: Path, story_id: str) -> list[tuple[str, int]]:
        self.webdriver.get("file://" + str(xml_path))
        body = self.webdriver.find_element(By.TAG_NAME, "body")
        content_ids = []
        spm_list: list[tuple[str, int]] = []
        for child in body.find_elements(By.XPATH, "*"):
            content_id = self.process_tag(child, story_id, spm_list)
            if content_id is not None:
                content_ids.append(content_id)
        content_str = ", ".join(f'kfx_id::"{content_id}"' for content_id in content_ids)
        storyline_ion = f"""{{
  story_name: kfx_id::"{story_id}",
  content_list: [{content_str}]
}}"""
        self.insert_blob_fragment(story_id, storyline_ion, "storyline")
        self.insert_fragment_properties(
            [(story_id, "element_type", "storyline"), (story_id, "child", story_id)]
        )
        return spm_list

    def process_tag(
        self, tag: WebElement, parent_id: str, spm_list: list[tuple[str, int]]
    ) -> str | None:
        if not is_tag_displayed(tag):
            return None
        if tag.tag_name in ["figure", "img"]:
            return None
        elif is_block_tag(tag):
            if not contain_block_tag(tag):
                if len(tag.text) > 0:
                    return self.create_text_structure(tag, parent_id, spm_list)
            else:
                return self.create_container_structure(tag, parent_id, spm_list)
        return None

    def create_container_structure(
        self, tag: WebElement, parent_id: str, spm_list: list[tuple[str, int]]
    ) -> str:
        structure_id = self.create_fragment_id("i")
        spm_list.append((structure_id, 1))
        content_ids = []
        for child in tag.find_elements(By.XPATH, "*"):
            content_id = self.process_tag(child, structure_id, spm_list)
            if content_id is not None:
                content_ids.append(content_id)

        content_str = ", ".join(f'kfx_id::"{content_id}"' for content_id in content_ids)
        structure_ion = f"""{{
  kfx_id: kfx_id::"{structure_id}",
  type: container,
  content_list: [{content_str}],
}}"""
        self.insert_blob_fragment(structure_id, structure_ion, "structure")
        self.insert_fragment_properties(
            [
                (parent_id, "child", structure_id),
                (structure_id, "element_type", "structure"),
            ]
        )
        return structure_id

    def create_text_structure(
        self, tag: WebElement, parent_id: str, spm_list: list[tuple[str, int]]
    ) -> str:
        structure_id = self.create_fragment_id("i")
        ion = IonPyDict.from_value(
            IonType.STRUCT,
            {
                "kfx_id": IonPyText.from_value(
                    IonType.STRING, structure_id, ("kfx_id",)
                ),
                "type": IonPySymbol.from_value(IonType.SYMBOL, "text"),
                "content": tag.text,
            },
            ("structure",),
        )
        self.insert_blob_fragment(structure_id, ion)
        self.insert_fragment_properties(
            [
                (parent_id, "child", structure_id),
                (structure_id, "element_type", "structure"),
            ]
        )
        spm_list.append((structure_id, len(tag.text)))
        return structure_id

    def create_document_data(self, section_ids: list[str]) -> None:
        section_ion_str = ",".join(
            f'kfx_id::"{section_id}"' for section_id in section_ids
        )
        document_data_ion = f"""{{
  direction: ltr,
  writing_mode: horizontal_tb,
  column_count: auto,
  selection: enabled,
  spacing_percent_base: width,
  reading_orders: [
    {{
      reading_order_name: default,
      sections: [{section_ion_str}]
    }}
  ]
}}"""
        self.insert_blob_fragment("document_data", document_data_ion, "document_data")
        self.insert_fragment_property("document_data", "element_type", "document_data")
        metadata_ion = f"""{{
  reading_orders: [
    {{
      reading_order_name: default,
      sections: [{section_ion_str}]
    }}
  ]
}}"""
        self.insert_blob_fragment("metadata", metadata_ion, "metadata")
        self.insert_fragment_property("metadata", "element_type", "metadata")

    def create_book_navigation(self, structure_ids: dict[str, str]) -> None:
        from .epub import NAMESPACES

        if self.epub_metadata.toc is None:
            return None
        toc_root = etree.parse(self.epub_metadata.toc)
        nav_containers = []
        for ol_tag in toc_root.iterfind(".//xml:nav/xml:ol", NAMESPACES):
            nav_entries = self.create_nav_entries(ol_tag, structure_ids)
            nav_container_id = self.create_fragment_id("n")
            nav_container_ion = IonPyDict.from_value(
                IonType.STRUCT,
                {
                    "nav_type": IonPySymbol.from_value(IonType.SYMBOL, "toc"),
                    "nav_container_name": IonPyText.from_value(
                        IonType.STRING, nav_container_id, ("kfx_id",)
                    ),
                    "entries": nav_entries,
                },
                ("nav_container",),
            )
            nav_containers.append(nav_container_ion)

        nav_ion = IonPyList.from_value(
            IonType.LIST,
            [
                {
                    "reading_order_name": IonPySymbol.from_value(
                        IonType.SYMBOL, "default"
                    ),
                    "nav_containers": nav_containers,
                }
            ],
            ("book_navigation",),
        )
        self.insert_blob_fragment("book_navigation", nav_ion)
        self.insert_fragment_property(
            "book_navigation", "element_type", "book_navigation"
        )

    def create_nav_entries(
        self, ol_tag: etree._Element, structure_ids: dict[str, str]
    ) -> list[IonPyDict]:
        from .epub import NAMESPACES

        entries: list[IonPyDict] = []
        for li_tag in ol_tag.iterfind("xml:li", NAMESPACES):
            a_tag = li_tag.find("xml:a", NAMESPACES)
            if a_tag is None:
                continue
            nested_entries: list[IonPyDict] = []
            nested_ol_tag = li_tag.find("xml:ol", NAMESPACES)
            if nested_ol_tag is not None:
                nested_entries = self.create_nav_entries(nested_ol_tag, structure_ids)
            label = a_tag.xpath("string()")
            href = Path(a_tag.get("href", "")).name
            if href in structure_ids:
                nav_unit_data: dict[str, Any] = {
                    "representation": {"label": label},
                    "target_position": {
                        "id": IonPyText.from_value(
                            IonType.STRING, structure_ids[href], ("kfx_id",)
                        ),
                        "offset": 0,
                    },
                }
                if len(nested_entries) > 0:
                    nav_unit_data["entries"] = nested_entries
                nav_unit_ion = IonPyDict.from_value(
                    IonType.STRUCT, nav_unit_data, ("nav_unit",)
                )
                entries.append(nav_unit_ion)
        return entries

    def create_section_pid_count_map(
        self, structure_ids: dict[str, list[tuple[str, int]]]
    ) -> None:
        section_lens = {}
        for section_id, spm_list in structure_ids.items():
            section_lens[section_id] = sum(s_len for _, s_len in spm_list)

        map_ion = IonPyDict.from_value(
            IonType.STRUCT,
            {
                "contains": [
                    {
                        "section_name": IonPyText.from_value(
                            IonType.STRING, section_id, ("kfx_id",)
                        ),
                        "length": section_len,
                    }
                    for section_id, section_len in section_lens.items()
                ]
            },
            ("yj.section_pid_count_map",),
        )
        self.insert_blob_fragment("yj.section_pid_count_map", map_ion)
        self.insert_fragment_property(
            "yj.section_pid_count_map", "element_type", "yj.section_pid_count_map"
        )

    def create_location_map(
        self, structure_ids: dict[str, list[tuple[str, int]]]
    ) -> None:
        locations = []
        for _, spm_list in structure_ids.items():
            offset = 0
            for structure_id, structure_len in spm_list:
                locations.append((structure_id, offset))
                offset += structure_len
        map_ion = IonPyDict.from_value(
            IonType.STRUCT,
            {
                "reading_order_name": IonPySymbol.from_value(IonType.SYMBOL, "default"),
                "locations": [
                    {
                        "id": IonPyText.from_value(
                            IonType.STRING, structure_id, ("kfx_id",)
                        ),
                        "offset": offset,
                    }
                    for structure_id, offset in locations
                ],
            },
            ("location_map",),
        )
        self.insert_blob_fragment("location_map", map_ion)
        self.insert_fragment_property("location_map", "element_type", "location_map")


def remove_ion_table(binary: bytes) -> bytes:
    """
    Remove the extra import structure added by the "imports" arguments
    in `simpleion.dumps`.
    """
    return b"\xe0\x01\x00\xea" + binary[36:]


def int_to_base32(num: int) -> str:
    if num == 0:
        return "0"
    # no "I", "L", "O", "Q"
    symbols = "0123456789ABCDEFGHJKMNPRSTUVWXYZ"
    digits = []
    while num > 0:
        digits.append(symbols[num % 32])
        num //= 32
    digits.reverse()
    return "".join(digits)


def init_webdriver() -> WebDriver:
    from selenium import webdriver
    from selenium.webdriver.firefox.firefox_profile import FirefoxProfile

    options = webdriver.FirefoxOptions()
    firefox_profile = FirefoxProfile()
    firefox_profile.set_preference("javascript.enabled", False)
    options.profile = firefox_profile
    options.add_argument("-headless")
    return webdriver.Firefox(options=options)


def contain_block_tag(tag: WebElement) -> bool:
    for child in tag.find_elements(By.XPATH, "*"):
        if is_block_tag(child):
            return True
        elif contain_block_tag(child):
            return True
    return False


def is_block_tag(tag: WebElement) -> bool:
    return tag.value_of_css_property("display") == "block"


def is_tag_displayed(tag: WebElement) -> bool:
    return tag.is_displayed() and tag.value_of_css_property("font-size") != "0px"

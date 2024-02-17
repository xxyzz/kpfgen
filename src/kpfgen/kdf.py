from pathlib import Path


class KDF:
    def __init__(self):
        self.create_symbol_catalog()
        self.fragment_id = 0

    def create_symbol_catalog(self):
        from amazon.ion.symbols import SymbolTableCatalog, shared_symbol_table

        from .yj_symbols import YJ_CONVERSION_SYMBOLS, YJ_SYMBOLS

        self.symbol_table = shared_symbol_table(
            "YJ_symbols", 10, YJ_SYMBOLS + YJ_CONVERSION_SYMBOLS
        )
        self.catalog = SymbolTableCatalog()
        self.catalog.register(self.symbol_table)

    def create_kdf(self, tmp_dir: Path, db_path: Path):
        from .epub import get_epub_metadata

        self.res_dir = db_path.parent / "res"
        self.res_dir.mkdir(exist_ok=True)
        db_path.unlink(True)
        self.create_kdf_tables(db_path)
        self.insert_ion_symbol_table()
        self.epub_metadata = get_epub_metadata(tmp_dir)
        self.insert_metadata()
        self.insert_cover_section()

        self.conn.commit()
        self.conn.close()

    def create_kdf_tables(self, db_path: Path):
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

    def insert_ion_symbol_table(self):
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

    def insert_blob_fragment(self, fragment_id: str, ion_text: str, annotation: str):
        from amazon.ion import simpleion
        from amazon.ion.core import IonType
        from amazon.ion.simple_types import IonPyDict

        value = simpleion.loads(ion_text, catalog=self.catalog)
        value = IonPyDict.from_value(IonType.STRUCT, value, (annotation,))
        self.insert_fragment(
            fragment_id,
            "blob",
            remove_ion_table(
                simpleion.dumps(value, binary=True, imports=(self.symbol_table,))
            ),
        )

    def insert_metadata(self):
        import random
        import string
        from importlib.metadata import version

        metadata_str = ""
        for metadata in ("language", "title", "description", "author", "publisher"):
            value = getattr(self.epub_metadata, metadata)
            if len(value) > 0:
                metadata_str += f""",
                {{
                  key: "{metadata}",
                  value: "{value}"
                }}"""

        ion_text = f"""{{
categorised_metadata: [
  {{
    category: "kindle_ebook_metadata",
    metadata: [
      {{
        key: "selection",
        value: "enabled"
      }},
      {{
        key: "nested_span",
        value: "enabled"
      }}
    ]
  }},
  {{
    category: "kindle_audit_metadata",
    metadata: [
      {{
        key: "file_creator",
        value: "kpfgen"
      }},
      {{
        key: "creator_version",
        value: "{version('kpfgen')}"
      }}
    ]
  }},
  {{
    category: "kindle_title_metadata",
    metadata: [
      {{
        key: "book_id",
        value: "{''.join(random.choices(string.digits + string.ascii_letters, k=23))}"
      }}
      {metadata_str}
    ]
  }}
]
}}"""
        self.insert_blob_fragment("book_metadata", ion_text, "book_metadata")
        self.insert_content_features()

    def insert_content_features(self):
        ion_text = """{
  kfx_id: content_features,
  features: [
    {
      namespace: "com.amazon.yjconversion",
      key: "reflow-style",
      version_info: {
        version: {
          major_version: 1,
          minor_version: 0
        }
      }
    }
  ]
}"""
        self.insert_blob_fragment("content_features", ion_text, "content_features")
        self.insert_fragment_property(
            "content_features", "element_type", "content_features"
        )

    def insert_cover_section(self):
        from PIL import Image

        if self.epub_metadata.cover_path is None:
            return
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
  font_size: {{
    value: 1.0e0,
    unit: rem
  }},
  line_height: {{
    value: 1.0e0,
    unit: lh
  }},
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
  contains: [
    [
      1,
      kfx_id::"{section_struct_id}"
    ],
    [
      2,
      kfx_id::"{story_id}"
    ]
  ]
}}"""
        spm_id = f"{section_id}-spm"
        self.insert_blob_fragment(spm_id, spm_text, "section_position_id_map")
        self.insert_fragment_property(spm_id, "element_type", "section_position_id_map")
        self.insert_section_auxiliary_data(section_id)

    def create_fragment_id(self, prefix: str) -> str:
        fragment_id_str = prefix + int_to_base32(self.fragment_id)
        self.fragment_id += 1
        return fragment_id_str

    def insert_image_resource(self, image_path: Path) -> str:
        import shutil

        from PIL import Image

        with Image.open(image_path) as im:
            im_width, im_height = im.size
            im_format = im.format or ""
            im_format = im_format.lower()
            if im_format == "jpeg":
                im_format = "jpg"

        res_id = self.create_fragment_id("e")
        res_loc_id = self.create_fragment_id("rsrc")
        res_text = f"""{{
  format: {im_format},
  location: "{res_loc_id}",
  'yj.conversion.source_resource_filename': "{image_path.name}",
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
        self.insert_fragment(res_loc_id, "path", f"rsc/{res_loc_id}")
        self.insert_fragment_property(res_loc_id, "element_type", "bcRawMedia")

        return res_id

    def insert_fragment(
        self, fragment_id: str, payload_type: str, payload_value: str | bytes
    ):
        self.conn.execute(
            "INSERT INTO fragments VALUES(?, ?, ?)",
            (fragment_id, payload_type, payload_value),
        )

    def insert_fragment_property(self, fragment_id: str, key: str, value: str):
        self.conn.execute(
            "INSERT INTO fragment_properties VALUES(?, ?, ?)",
            (fragment_id, key, value),
        )

    def insert_fragment_properties(self, data):
        self.conn.executemany("INSERT INTO fragment_properties VALUES(?, ?, ?)", data)

    def insert_section_auxiliary_data(self, section_id: str):
        ad_id = section_id + "-ad"
        ion_text = f"""{{
  kfx_id: kfx_id::"{ad_id}",
  metadata: [
    {{
      key: "IS_TARGET_SECTION",
      value: true
    }}
  ]
}}"""
        self.insert_blob_fragment(ad_id, ion_text, "auxiliary_data")
        self.insert_fragment_properties(
            [
                (section_id, "child", ad_id),
                (ad_id, "element_type", "auxiliary_data"),
            ]
        )


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

import sqlite3
from pathlib import Path

from amazon.ion import simpleion
from amazon.ion.core import IonType
from amazon.ion.simple_types import IonPyDict
from amazon.ion.symbols import SymbolTable, SymbolTableCatalog, shared_symbol_table

from .epub import EPUBMetadata


def create_kdf_tables(conn: sqlite3.Connection):
    conn.executescript(
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


def remove_ion_table(binary: bytes) -> bytes:
    """
    Remove the extra import structure added by the "imports" arguments
    in `simpleion.dumps`.
    """
    return b"\xe0\x01\x00\xea" + binary[36:]


def create_symbol_catalog(
    conn: sqlite3.Connection,
) -> tuple[SymbolTable, SymbolTableCatalog]:
    from .yj_symbols import YJ_SYMBOLS

    extra_symbols = (
        "yj.conversion.offset_map",
        "yj.conversion.line_no",
        "yj.conversion.amzn_src_id",
        "yj.conversion.offset",
        "yj.conversion.source_style_width",
        "yj.conversion.modified_content_info",
        "yj.conversion.added_content",
        "yj.conversion.type",
        "yj.conversion.length",
        "yj.conversion.content",
        "yj.conversion.html_name",
        "yj.conversion.source_resource_filename",
        "yj.conversion.source_style_height",
    )
    table = shared_symbol_table("YJ_symbols", 10, YJ_SYMBOLS + extra_symbols)
    catalog = SymbolTableCatalog()
    catalog.register(table)

    max_id = 9 + len(YJ_SYMBOLS) + len(extra_symbols)
    value = IonPyDict.from_value(
        IonType.STRUCT,
        {
            "max_id": max_id,
            "imports": [
                {"name": "YJ_symbols", "version": 10, "max_id": len(YJ_SYMBOLS)}
            ],
            "symbols": extra_symbols,
        },
        ("$ion_symbol_table",),
    )
    binary = simpleion.dumps(value, binary=True, imports=(table,))
    conn.execute(
        "INSERT INTO fragments VALUES('$ion_symbol_table', 'blob', ?)",
        (remove_ion_table(binary),),
    )
    conn.execute(
        "INSERT INTO fragments VALUES('max_id', 'blob', ?)",
        (simpleion.dumps(max_id, binary=True),),
    )
    conn.executescript(
        """
        INSERT INTO fragment_properties
        VALUES('$ion_symbol_table', 'element_type', '$ion_symbol_table');
        INSERT INTO fragment_properties VALUES('max_id', 'element_type', 'max_id');
        """
    )
    return table, catalog


def insert_metadata(
    conn: sqlite3.Connection,
    epub_metadata: EPUBMetadata,
    symbol_table: SymbolTable,
    catalog: SymbolTableCatalog,
):
    import random
    import string
    from importlib.metadata import version

    metadata_str = ""
    for metadata in ("language", "title", "description", "author", "publisher"):
        value = getattr(epub_metadata, metadata)
        if len(value) > 0:
            metadata_str += f""",
            {{
              key: "{metadata}",
              value: "{value}"
            }}"""

    value = simpleion.loads(
        f"""{{
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
}}""",
        catalog=catalog,
    )
    value = IonPyDict.from_value(IonType.STRUCT, value, ("book_metadata",))
    conn.execute(
        "INSERT INTO fragments VALUES('book_metadata', 'blob', ?)",
        (
            remove_ion_table(
                simpleion.dumps(value, binary=True, imports=(symbol_table,))
            ),
        ),
    )
    conn.execute(
        """
        INSERT INTO fragment_properties
        VALUES('book_metadata', 'element_type', 'book_metadata')
        """
    )
    insert_content_features(conn, symbol_table, catalog)


def insert_content_features(
    conn: sqlite3.Connection,
    symbol_table: SymbolTable,
    catalog: SymbolTableCatalog,
):
    value = simpleion.loads(
        """{
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
}""",
        catalog=catalog,
    )
    value = IonPyDict.from_value(IonType.STRUCT, value, ("content_features",))
    conn.execute(
        "INSERT INTO fragments VALUES('content_features', 'blob', ?)",
        (
            remove_ion_table(
                simpleion.dumps(value, binary=True, imports=(symbol_table,))
            ),
        ),
    )
    conn.execute(
        """
        INSERT INTO fragment_properties
        VALUES('content_features', 'element_type', 'content_features')
        """
    )



def create_kdf(temp_dir: Path, db_path: Path):
    from .epub import get_epub_metadata

    db_path.unlink(True)
    kdf_conn = sqlite3.connect(db_path)
    create_kdf_tables(kdf_conn)
    symbol_table, symbol_catalog = create_symbol_catalog(kdf_conn)
    epub_metadata = get_epub_metadata(temp_dir)
    insert_metadata(kdf_conn, epub_metadata, symbol_table, symbol_catalog)

    kdf_conn.commit()
    kdf_conn.close()

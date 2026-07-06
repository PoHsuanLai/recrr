use recrr::{Crdt, CrdtTable, PkSpec};

// A junction table: composite key, no tracked columns.
#[derive(Crdt)]
#[crdt(table = "paper_collections", pk = (paper_id, collection_id; sep = ':'))]
struct PaperCollection {
    paper_id: String,
    collection_id: String,
}

fn main() {
    let spec = PaperCollection::table_spec();
    assert_eq!(spec.name, "paper_collections");
    assert!(spec.columns.is_empty());
    match spec.pk {
        PkSpec::Composite { columns, sep } => {
            assert_eq!(columns.0, "paper_id");
            assert_eq!(columns.1, "collection_id");
            assert_eq!(sep, ':');
        }
        _ => panic!("expected composite pk"),
    }
}

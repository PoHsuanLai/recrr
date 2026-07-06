use recrr::{Crdt, CrdtTable};

#[derive(Crdt)]
#[crdt(table = "papers")]
struct Paper {
    #[crdt(pk)]
    id: String,
    title: String,
    #[crdt(skeleton = "\"[]\"")]
    authors: String,
    #[crdt(rename = "is_favorite")]
    favorite: bool,
    #[crdt(skip)]
    local_cache: Option<String>,
}

fn main() {
    assert_eq!(Paper::TABLE, "papers");
    assert_eq!(Paper::TITLE, "title");
    assert_eq!(Paper::AUTHORS, "authors");
    assert_eq!(Paper::FAVORITE, "is_favorite");
    assert_eq!(Paper::ID, "id");
    assert_eq!(Paper::ALL, &["title", "authors", "is_favorite"]);

    let spec = Paper::table_spec();
    assert_eq!(spec.name, "papers");
    assert_eq!(spec.columns, vec!["title", "authors", "is_favorite"]);
}

use recrr::Crdt;

#[derive(Crdt)]
#[crdt(table = "papers")]
struct Paper {
    #[crdt(pk)]
    id: String,
    #[crdt(bogus = "x")]
    title: String,
}

fn main() {}

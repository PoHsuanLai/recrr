use recrr::Crdt;

#[derive(Crdt)]
#[crdt(table = "papers")]
struct Paper {
    title: String,
}

fn main() {}

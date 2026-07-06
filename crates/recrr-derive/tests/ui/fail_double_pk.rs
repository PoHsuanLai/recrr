use recrr::Crdt;

// Both a field pk and a composite pk — ambiguous.
#[derive(Crdt)]
#[crdt(table = "papers", pk = (a, b; sep = ':'))]
struct Paper {
    #[crdt(pk)]
    id: String,
    title: String,
}

fn main() {}

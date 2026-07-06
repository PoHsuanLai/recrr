use recrr::Crdt;

#[derive(Crdt)]
struct Paper {
    #[crdt(pk)]
    id: String,
    title: String,
}

fn main() {}

use recrr::Crdt;

#[derive(Crdt)]
#[crdt(table = "papers")]
struct Paper(String, String);

fn main() {}

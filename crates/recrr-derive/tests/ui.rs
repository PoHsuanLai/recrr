//! Compile-time UI tests for `#[derive(Crdt)]` diagnostics.
//!
//! Pass cases must compile; fail cases must produce the committed `.stderr`.
//! Regenerate snapshots after an intentional message change with
//! `TRYBUILD=overwrite cargo test -p recrr-derive`.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass_basic.rs");
    t.pass("tests/ui/pass_composite.rs");
    t.compile_fail("tests/ui/fail_missing_table.rs");
    t.compile_fail("tests/ui/fail_no_pk.rs");
    t.compile_fail("tests/ui/fail_double_pk.rs");
    t.compile_fail("tests/ui/fail_unknown_key.rs");
    t.compile_fail("tests/ui/fail_tuple_struct.rs");
}

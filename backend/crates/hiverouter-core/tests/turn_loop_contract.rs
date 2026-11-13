//! Budget and bounding contract tests for the 1.2 tool-loop primitives.
//!
//! The engine relies on these pure functions for the per-run caps: a bounded
//! result body must never exceed the advertised byte cap (including its
//! truncation marker), and the approval-wait budget must refuse exactly at the
//! frozen cap instead of letting a run arm one wait too many.

use hiverouter_core::tool_loop::{
    bounded_tool_text, check_approval_wait_budget, check_call_budget, next_turn,
    MAX_APPROVAL_WAITS_PER_RUN, MAX_CALLS_PER_RUN, MAX_TOOL_RESULT_BYTES, MAX_TOOL_TURNS,
};

#[test]
fn bounded_text_keeps_short_bodies_unchanged() {
    let text = "hello";
    assert_eq!(bounded_tool_text(text), text);
}

#[test]
fn bounded_text_stays_within_the_cap_including_its_marker() {
    let ascii = "a".repeat(MAX_TOOL_RESULT_BYTES * 2);
    let bounded = bounded_tool_text(&ascii);
    assert!(bounded.len() <= MAX_TOOL_RESULT_BYTES, "ascii overrun");
    assert!(bounded.ends_with(']'));
    assert!(bounded.contains("[truncated at"));

    // Multi-byte input: the cut must land on a char boundary and still fit.
    let multi = "é".repeat(MAX_TOOL_RESULT_BYTES);
    let bounded = bounded_tool_text(&multi);
    assert!(bounded.len() <= MAX_TOOL_RESULT_BYTES, "utf8 overrun");
    assert!(bounded.is_char_boundary(bounded.len()));
}

#[test]
fn approval_wait_budget_refuses_exactly_at_the_cap() {
    assert!(check_approval_wait_budget(0).is_ok());
    assert!(check_approval_wait_budget(MAX_APPROVAL_WAITS_PER_RUN - 1).is_ok());
    assert!(check_approval_wait_budget(MAX_APPROVAL_WAITS_PER_RUN).is_err());
    assert!(check_approval_wait_budget(u32::MAX).is_err());
}

#[test]
fn call_and_turn_budgets_refuse_at_their_caps() {
    assert!(check_call_budget(MAX_CALLS_PER_RUN - 1).is_ok());
    assert!(check_call_budget(MAX_CALLS_PER_RUN).is_err());
    assert_eq!(next_turn(MAX_TOOL_TURNS - 1), Ok(MAX_TOOL_TURNS));
    assert!(next_turn(MAX_TOOL_TURNS).is_err());
}

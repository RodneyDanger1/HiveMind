//! Finish-verification contract tests for the agent turn loop.
//!
//! These pin the pure `terminal_verified` / `shell_command_mutates` rules the
//! engine consults before it lets a model `finish` a task: a mutation needs a
//! later passing *full-suite* `run_tests`, a filtered/failed run does not
//! verify, and shell writes are mutations even when the program is normally
//! read-only.

use hiverouter_core::execution::{shell_command_mutates, terminal_verified, ToolTurnTranscript};

fn plan(tool: &str, args: serde_json::Value) -> ToolCallPlan {
    ToolCallPlan {
        tool_call_id: format!("call_{tool}"),
        tool: tool.to_owned(),
        args,
        args_sha256: "0".repeat(64),
        turn: 1,
    }
}

fn write() -> ToolCallPlan {
    plan("file_write", serde_json::json!({"path": "src/lib.rs", "content": "x"}))
}

fn shell(program: &str, args: Vec<&str>) -> ToolCallPlan {
    plan(
        "shell_exec",
        serde_json::json!({"program": program, "args": args}),
    )
}

fn tests_pass(args: serde_json::Value) -> (ToolCallPlan, ToolExecution) {
    (
        plan("run_tests", args),
        ToolExecution::ok(
            "call_run_tests",
            r#"{"success":true,"exit_code":0,"stdout":"ok"}"#.to_owned(),
        ),
    )
}

fn tests_fail() -> (ToolCallPlan, ToolExecution) {
    (
        plan("run_tests", serde_json::json!({"args": []})),
        ToolExecution::ok(
            "call_run_tests",
            r#"{"success":false,"exit_code":101,"stdout":"failed"}"#.to_owned(),
        ),
    )
}

fn ok(plan: ToolCallPlan) -> (ToolCallPlan, ToolExecution) {
    let id = plan.tool_call_id.clone();
    (plan, ToolExecution::ok(id, "done".to_owned()))
}

#[test]
fn write_then_failing_tests_does_not_verify() {
    let current = vec![ok(write()), tests_fail()];
    assert!(!terminal_verified(&[], &current));
}

#[test]
fn write_then_passing_failed_exit_code_does_not_verify() {
    let (test_plan, mut test_result) = tests_pass(serde_json::json!({"args": []}));
    test_result.text = r#"{"success":true,"exit_code":2}"#.to_owned();
    assert!(!terminal_verified(&[], &[(ok(write())), (test_plan, test_result)]));
}

#[test]
fn write_then_passing_full_suite_verifies() {
    let current = vec![ok(write()), tests_pass(serde_json::json!({"args": []}))];
    assert!(terminal_verified(&[], &current));
}

#[test]
fn write_then_filtered_passing_suite_does_not_verify() {
    let current = vec![
        ok(write()),
        tests_pass(serde_json::json!({"args": ["--lib"]})),
    ];
    assert!(!terminal_verified(&[], &current));
}

#[test]
fn verification_in_a_prior_turn_is_revoked_by_a_later_mutation() {
    let prior = vec![ToolTurnTranscript {
        assistant_text: String::new(),
        reasoning: String::new(),
        results: vec![ok(write()), tests_pass(serde_json::json!({"args": []}))],
    }];
    assert!(terminal_verified(&prior, &[]));
    assert!(!terminal_verified(&prior, &[ok(write())]));
}

#[test]
fn refused_mutation_is_not_a_side_effect() {
    let refused = (
        write(),
        ToolExecution::refused("call_file_write", "E_TOOL_NOT_ALLOWED", "no".to_owned()),
    );
    assert!(terminal_verified(&[], &[refused]));
}

#[test]
fn shell_file_writers_are_treated_as_mutations() {
    for program in ["rm", "del", "mv", "cp", "tee", "Set-Content", "Remove-Item"] {
        let call = shell(program, vec!["target"]);
        assert!(
            shell_command_mutates(&call),
            "{program} must count as a workspace mutation"
        );
    }
}

#[test]
fn shell_redirection_is_a_mutation_but_descriptor_redirect_is_not() {
    assert!(shell_command_mutates(&shell(
        "echo",
        vec!["x", ">", "out.txt"]
    )));
    assert!(!shell_command_mutates(&shell(
        "ls",
        vec!["missing", "2>&1"]
    )));
}

#[test]
fn read_only_shell_and_git_queries_are_not_mutations() {
    assert!(!shell_command_mutates(&shell("git", vec!["status"])));
    assert!(!shell_command_mutates(&shell("cargo", vec!["test"])));
    assert!(!shell_command_mutates(&shell("ls", vec!["-la"])));
    assert!(shell_command_mutates(&shell("git", vec!["commit", "-m", "x"])));
    assert!(shell_command_mutates(&shell("cargo", vec!["fmt"])));
    assert!(shell_command_mutates(&shell("git", vec!["-C", "repo", "reset"])));
}

#[test]
fn shell_mutation_requires_a_later_passing_full_suite() {
    let current = vec![
        ok(shell("rm", vec!["-rf", "dist"])),
        tests_pass(serde_json::json!({"args": []})),
    ];
    assert!(terminal_verified(&[], &current));
    assert!(!terminal_verified(
        &[],
        &[
            ok(shell("rm", vec!["-rf", "dist"])),
            tests_pass(serde_json::json!({"args": ["--test", "one"]})),
        ]
    ));
}

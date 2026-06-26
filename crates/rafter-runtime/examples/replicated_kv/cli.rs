use std::path::PathBuf;

use rafter::NodeId;

use super::{
    in_process::run_in_process_demo,
    process::{run_process_demo_with_spawn, run_process_node, ProcessSpawn},
    types::ScenarioOptions,
};

pub(crate) fn run(args: &[String]) {
    let keep_dir = args.iter().any(|arg| arg == "--keep-dir");
    if args.iter().any(|arg| arg == "--process-node") {
        let node_id = NodeId(
            argument_after(args, "--id")
                .expect("process node id")
                .parse()
                .expect("process node id parses"),
        );
        let root =
            PathBuf::from(argument_after(args, "--root").expect("process node root directory"));
        run_process_node(&root, node_id);
        return;
    }

    if args.iter().any(|arg| arg == "--process-cluster") {
        let root = argument_after(args, "--root")
            .map_or_else(|| default_root("replicated-kv-process"), PathBuf::from);
        let report = run_process_demo_with_spawn(
            root,
            ScenarioOptions {
                keep_dir,
                verbose: true,
            },
            ProcessSpawn::example_binary(std::env::current_exe().expect("current executable")),
        );
        assert_eq!(report.final_values.get("delta"), Some(&"4".to_string()));
        return;
    }

    let root =
        argument_after(args, "--root").map_or_else(|| default_root("replicated-kv"), PathBuf::from);
    let report = run_in_process_demo(
        root.clone(),
        ScenarioOptions {
            keep_dir,
            verbose: true,
        },
    );
    if keep_dir {
        println!("kept example directory: {}", root.display());
    }
    assert_eq!(report.final_values.get("delta"), Some(&"4".to_string()));
}

fn argument_after(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find_map(|window| (window[0] == flag).then(|| window[1].clone()))
}

fn default_root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("rafter-{label}-{}", std::process::id()))
}

//! Complete aggregate report fixtures used to exercise always-publish behavior.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use super::CANONICAL_INVARIANT_IDS;

static NEXT_REPORT_FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) struct AggregateReportFixture {
    root: PathBuf,
    runner_temp: PathBuf,
    report_dir: String,
    pub(crate) github_output: PathBuf,
    pub(crate) github_summary: PathBuf,
    profile: String,
    pub(crate) markdown: String,
}

impl AggregateReportFixture {
    pub(crate) fn new(workspace: &Path, profile: &str) -> Self {
        let id = NEXT_REPORT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        let root = workspace
            .join("target/ci-contract")
            .join(format!("always-publish-{}-{id}", std::process::id()));
        if root.exists() {
            fs::remove_dir_all(&root).expect("remove stale aggregate fixture");
        }
        let runner_temp = root.join("runner-temp");
        let report_dir = format!("reports-{profile}");
        let reports = runner_temp.join(&report_dir);
        fs::create_dir_all(&reports).expect("create aggregate report fixture");

        fs::create_dir_all(root.join("artifacts/invariants"))
            .expect("create aggregate evidence fixture");
        fs::write(root.join("artifacts/invariants/evidence.json"), "{}\n")
            .expect("write aggregate evidence fixture");
        fs::create_dir_all(root.join("target/rafter-invariants/telemetry"))
            .expect("create aggregate telemetry fixture");
        fs::write(
            root.join("target/rafter-invariants/telemetry/process.log"),
            "fixture\n",
        )
        .expect("write aggregate telemetry fixture");

        let fixture = Self {
            github_output: root.join("github-output"),
            github_summary: root.join("github-summary"),
            root,
            runner_temp,
            report_dir,
            profile: profile.to_owned(),
            markdown: render_markdown(profile, &CANONICAL_INVARIANT_IDS),
        };
        fixture.write_reports(
            &CANONICAL_INVARIANT_IDS,
            &CANONICAL_INVARIANT_IDS,
            &CANONICAL_INVARIANT_IDS,
        );
        fixture
    }

    pub(crate) fn write_reports(
        &self,
        json_ids: &[&str],
        markdown_ids: &[&str],
        junit_ids: &[&str],
    ) {
        let reports = self.runner_temp.join(&self.report_dir);
        let invariants = json_ids
            .iter()
            .map(|id| format!(r#"{{"invariant_id":"{id}"}}"#))
            .collect::<Vec<_>>()
            .join(",");
        fs::write(
            reports.join(format!("{}.json", self.profile)),
            format!(
                r#"{{"profile":"{}","summary":{{"total":44,"green":44,"red":0}},"invariants":[{invariants}]}}"#,
                self.profile
            ),
        )
        .expect("write aggregate JSON fixture");
        fs::write(
            reports.join(format!("{}.md", self.profile)),
            render_markdown(&self.profile, markdown_ids),
        )
        .expect("write aggregate Markdown fixture");
        let junit_rows = junit_ids
            .iter()
            .map(|id| {
                format!("  <testcase classname=\"rafter.invariants\" name=\"{id}\">\n  </testcase>")
            })
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(
            reports.join(format!("{}.xml", self.profile)),
            format!("<testsuite tests=\"44\" failures=\"0\">\n{junit_rows}\n</testsuite>\n"),
        )
        .expect("write aggregate JUnit fixture");
    }

    pub(crate) fn environment<'a>(
        &'a self,
        extra: &[(&'a str, &'a str)],
    ) -> Vec<(&'a str, &'a str)> {
        let mut environment = vec![
            (
                "RUNNER_TEMP",
                self.runner_temp.to_str().expect("UTF-8 runner temp"),
            ),
            ("INVARIANT_REPORT_DIR", self.report_dir.as_str()),
            (
                "GITHUB_OUTPUT",
                self.github_output.to_str().expect("UTF-8 output path"),
            ),
            (
                "GITHUB_STEP_SUMMARY",
                self.github_summary.to_str().expect("UTF-8 summary path"),
            ),
        ];
        environment.extend_from_slice(extra);
        environment
    }
}

fn render_markdown(profile: &str, ids: &[&str]) -> String {
    let rows = ids
        .iter()
        .map(|id| format!("| `{id}` | GREEN | 1/1 | 1/1 | |"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "# Rafter invariant report: {profile}\n\n| Invariant | Verdict | Clauses | Evidence | Detail |\n| --- | --- | ---: | ---: | --- |\n{rows}\n"
    )
}

impl Drop for AggregateReportFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

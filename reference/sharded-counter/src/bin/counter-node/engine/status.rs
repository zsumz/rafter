//! Process status, audit projection, and ordered shutdown reporting.

use rafter::Role;

use super::{Engine, PendingAdmission};

impl Engine {
    pub(super) fn status_line(&self) -> String {
        let metrics = self.host.managed_metrics();
        let raft = self.host.raft_metrics();
        let leaders = raft
            .groups
            .iter()
            .filter(|metrics| {
                metrics.role == Role::Leader && !self.poisoned.contains(&metrics.group_id)
            })
            .count();
        let leader_groups = raft
            .groups
            .iter()
            .filter(|metrics| {
                metrics.role == Role::Leader && !self.poisoned.contains(&metrics.group_id)
            })
            .map(|metrics| metrics.group_id.get().to_string())
            .collect::<Vec<_>>()
            .join(",");
        let leader_groups = if leader_groups.is_empty() {
            "-"
        } else {
            &leader_groups
        };
        let durable_outstanding = self
            .groups
            .values()
            .map(|entry| entry.record.policy().outstanding.len())
            .sum::<usize>();
        let admission_candidates = self
            .pending_admissions
            .values()
            .map(PendingAdmission::candidate_count)
            .sum::<usize>();
        let admission_successors = self
            .pending_admissions
            .values()
            .map(PendingAdmission::successor_count)
            .sum::<usize>();
        let link = self.link.counters();
        format!(
            "OK STATUS ready={} groups={} leaders={} leader_groups={} poisoned={} queued={} \
             in_flight={} workers={} admitted={} client_admitted={} serviced={} failed={} \
             passes={} pending_proposals={} admission_reads={} admission_candidates={} \
             admission_successors={} admission_barriers={} durable_outstanding={} \
             recovery_deferred={} recovery_refused={} refused_peer={} \
             link_outbound_full={} link_inbound_full={} link_malformed={} \
             link_identity_refused={} link_inbound_connection_full={} \
             link_tls_handshakes={} link_tls_failures={} link_stale_sessions={} \
             link_active_outbound={} link_active_inbound={}",
            self.all_active_ready(),
            self.active_group_count(),
            leaders,
            leader_groups,
            self.poisoned.len(),
            metrics.queued,
            metrics.in_flight_work,
            metrics.occupied_workers,
            metrics.admitted,
            self.client_admitted,
            metrics.serviced,
            metrics.failed,
            metrics.passes_completed,
            self.pending_operations.len(),
            self.pending_admissions.len(),
            admission_candidates,
            admission_successors,
            self.admission_barriers_started,
            durable_outstanding,
            self.deferred_recovery.len(),
            self.recovery_refused,
            self.refused_peer,
            link.outbound_full,
            link.inbound_full,
            link.malformed,
            link.identity_refused,
            link.inbound_connection_full,
            link.tls_handshakes,
            link.tls_failures,
            link.stale_sessions,
            link.active_outbound_connections,
            link.active_inbound_connections
        )
    }

    pub(super) fn audit_line(&self) -> String {
        let metrics = self.host.managed_metrics();
        let (coverage, widest_gap) = self.audit.fairness();
        let conserved = metrics.admitted
            == metrics.serviced
                + metrics.failed
                + metrics.queued as u64
                + metrics.in_flight_work as u64;
        format!(
            "OK AUDIT plans={} passes_completed={} certified_passes={} opportunities={} \
             coverage={} widest_gap={} invalid_plans={} invalid_turns={} plan_digest={:016x} \
             turn_digest={:016x} admitted={} serviced={} failed={} queued={} in_flight={} \
             conserved={conserved}",
            self.audit.plans,
            self.audit.passes_completed,
            self.audit.certified_passes,
            self.audit.opportunities,
            coverage,
            widest_gap,
            self.audit.invalid_plans,
            self.audit.invalid_turns,
            self.audit.plan_digest,
            self.audit.turn_digest,
            metrics.admitted,
            metrics.serviced,
            metrics.failed,
            metrics.queued,
            metrics.in_flight_work
        )
    }

    pub(super) fn finish(&mut self) {
        for (read_id, pending) in std::mem::take(&mut self.pending_admissions) {
            if let Some(driver) = self
                .groups
                .get(&pending.group_id())
                .and_then(|entry| entry.driver.as_ref())
            {
                driver.cancel_read(read_id);
            }
            Self::finish_pending_admission(pending, "process shutting down");
        }
        self.pending_admission_operations.clear();
        for (_, pending) in std::mem::take(&mut self.pending) {
            for reply in pending.replies {
                reply.send("ERR UNKNOWN process shutting down".to_string(), false);
            }
        }
        self.pending_operations.clear();
        let audit = self.audit_line();
        let status = self.status_line();
        super::super::emit(&format!("FINAL {} {status}", self.node_id.0));
        super::super::emit(&format!("FINAL {} {audit}", self.node_id.0));
        self.link.shut_down();
        super::super::emit(&format!("STOPPED {}", self.node_id.0));
    }
}

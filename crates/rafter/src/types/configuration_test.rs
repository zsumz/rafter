use crate::{
    ConfigurationEntry, ConfigurationId, ConfigurationPhase, JointMembership, LogEntry,
    LogEntryKind, MembershipConfig, MembershipSet, NodeId,
};

#[test]
fn configuration_entries_distinguish_stable_and_joint_phases() {
    let stable_membership = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
        .expect("stable membership is valid");
    let stable = ConfigurationEntry::stable(ConfigurationId(7), stable_membership.clone());

    let old = stable_membership;
    let new = MembershipSet::new(vec![NodeId(2), NodeId(3), NodeId(4)], vec![NodeId(5)])
        .expect("new membership is valid");
    let joint_membership = JointMembership::new(old.clone(), new.clone());
    let joint = ConfigurationEntry::joint(ConfigurationId(8), joint_membership);

    assert_eq!(stable.phase(), ConfigurationPhase::Stable);
    assert_eq!(joint.phase(), ConfigurationPhase::Joint);
    assert_eq!(stable.config_id(), ConfigurationId(7));
    assert_eq!(joint.config_id(), ConfigurationId(8));
    assert_eq!(
        stable.membership_config(),
        MembershipConfig::stable(old.clone())
    );
    assert_eq!(joint.membership_config(), MembershipConfig::joint(old, new));
}

#[test]
fn log_entry_kind_keeps_application_payloads_separate_from_configuration_entries() {
    let application = LogEntryKind::application(vec![1, 2, 3, 5, 8]);
    let membership =
        MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("single voter membership is valid");
    let configuration =
        LogEntryKind::configuration(ConfigurationEntry::stable(ConfigurationId(1), membership));

    assert!(application.is_application());
    assert!(!application.is_configuration());
    assert_eq!(
        application.application_payload(),
        Some([1, 2, 3, 5, 8].as_slice())
    );
    assert!(application.configuration_entry().is_none());

    assert!(configuration.is_configuration());
    assert!(!configuration.is_application());
    assert_eq!(configuration.application_payload(), None);
    assert_eq!(
        configuration
            .configuration_entry()
            .expect("configuration entry is present")
            .phase(),
        ConfigurationPhase::Stable
    );
}

#[test]
fn application_log_entry_kind_accepts_borrowed_payloads() {
    let payload = [13, 21, 34, 55];

    let kind = LogEntryKind::application(payload.as_slice());
    let entry = LogEntry::application(crate::Term(1), payload.as_slice());

    assert_eq!(kind.application_payload(), Some(payload.as_slice()));
    assert_eq!(entry.application_payload(), Some(payload.as_slice()));
}

#[test]
fn configuration_domain_values_have_stable_display_debug_and_ordering() {
    assert_eq!(ConfigurationId(42).to_string(), "config-42");
    assert_eq!(ConfigurationPhase::Stable.to_string(), "stable");
    assert_eq!(ConfigurationPhase::Joint.to_string(), "joint");
    assert_eq!(format!("{:?}", ConfigurationId(42)), "ConfigurationId(42)");
    assert!(ConfigurationId(42).next() > ConfigurationId(42));
}

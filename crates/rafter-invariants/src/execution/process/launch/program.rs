//! Launcher protocol names, programs, and target-environment boundary.

use std::{borrow::Cow, collections::BTreeMap, error::Error};

pub(super) const RESOURCE_FD_ENV: &str = "RAFTER_INVARIANT_RESOURCE_FD";
pub(super) const TARGET_GROUP_ACK_FD_ENV: &str = "RAFTER_INVARIANT_TARGET_GROUP_ACK_FD";
pub(super) const TARGET_GROUP_FD_ENV: &str = "RAFTER_INVARIANT_TARGET_GROUP_FD";
pub(super) const TARGET_GROUP_ID_ENV: &str = "RAFTER_INVARIANT_TARGET_GROUP_ID";
pub(super) const TARGET_LIFETIME_LEASE_FD_ENV: &str = "RAFTER_INVARIANT_TARGET_LIFETIME_LEASE_FD";
pub(super) const INHERITED_FD_MAX_ENV: &str = "RAFTER_INVARIANT_INHERITED_FD_MAX";
pub(super) const INHERITED_FDS_ENV: &str = "RAFTER_INVARIANT_INHERITED_FDS";
pub(super) const WORKING_DIRECTORY_FD_ENV: &str = "RAFTER_INVARIANT_WORKING_DIRECTORY_FD";

#[cfg(test)]
const TEST_TARGET_LIFETIME_LEASE_FD_ENV: &str = "RAFTER_TEST_TARGET_LIFETIME_LEASE_FD";

#[cfg(test)]
thread_local! {
    static EXPOSE_NEXT_TARGET_LIFETIME_LEASE: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

pub(super) const RESOURCE_WRAPPER: &str = r#"
my $resource_fd = delete $ENV{'RAFTER_INVARIANT_RESOURCE_FD'};
my $inherited_fd_max = delete $ENV{'RAFTER_INVARIANT_INHERITED_FD_MAX'};
my $inherited_fds = delete $ENV{'RAFTER_INVARIANT_INHERITED_FDS'};
defined($inherited_fds) && $inherited_fds =~ /^\d+(?:,\d+)*$/
    or die "invalid inherited descriptor inventory";
my %keep = map { $_ => 1 } (0, 1, 2, split(/,/, $inherited_fds));
my $descriptor_directory = -d '/proc/self/fd' ? '/proc/self/fd' : '/dev/fd';
opendir(my $descriptors, $descriptor_directory)
    or die "open descriptor inventory $descriptor_directory: $!";
my @open_descriptors = grep { /^\d+$/ } readdir($descriptors);
closedir($descriptors) or die "close descriptor inventory: $!";
for my $descriptor (@open_descriptors) {
    next if $keep{$descriptor};
    POSIX::close($descriptor);
}
my $target_stderr_fd = POSIX::dup(2);
$target_stderr_fd >= 0 or die "duplicate target stderr: $!";
$ENV{'RAFTER_INVARIANT_TARGET_STDERR_FD'} = "$target_stderr_fd";
$inherited_fd_max = $target_stderr_fd
    if !defined($inherited_fd_max) || $target_stderr_fd > $inherited_fd_max;
$ENV{'RAFTER_INVARIANT_INHERITED_FD_MAX'} = "$inherited_fd_max";
$^F = $inherited_fd_max if $inherited_fd_max > $^F;
POSIX::dup2($resource_fd, 2) == 2 or die "redirect resource telemetry: $!";
POSIX::close($resource_fd) == 0 or die "close resource descriptor: $!";
my $time = shift @ARGV;
exec {$time} $time, @ARGV or die "exec $time: $!";
"#;

pub(super) const TARGET_GROUP_LAUNCHER: &str = r#"
my $group_fd = delete $ENV{'RAFTER_INVARIANT_TARGET_GROUP_FD'};
my $group_ack_fd = delete $ENV{'RAFTER_INVARIANT_TARGET_GROUP_ACK_FD'};
my $target_group = delete $ENV{'RAFTER_INVARIANT_TARGET_GROUP_ID'};
my $inherited_fd_max = delete $ENV{'RAFTER_INVARIANT_INHERITED_FD_MAX'};
my $target_stderr_fd = delete $ENV{'RAFTER_INVARIANT_TARGET_STDERR_FD'};
my $target_lifetime_lease_fd = delete $ENV{'RAFTER_INVARIANT_TARGET_LIFETIME_LEASE_FD'};
my $working_directory_fd = delete $ENV{'RAFTER_INVARIANT_WORKING_DIRECTORY_FD'};
$^F = $inherited_fd_max if defined($inherited_fd_max) && $inherited_fd_max > $^F;
open(my $group, '>&=', $group_fd) or die "open process-group receipt: $!";
my $selected = select($group);
$| = 1;
select($selected);
print {$group} "$$\n" or die "write process-group receipt: $!";
open(my $group_ack, '<&=', $group_ack_fd) or die "open process-group acknowledgement: $!";
open(my $target_lifetime_lease, '>&=', $target_lifetime_lease_fd)
    or die "open target lifetime lease: $!";
my $ack = '';
sysread($group_ack, $ack, 1) == 1 && $ack eq 'G'
    or die "read process-group acknowledgement: $!";
defined($target_group) && $target_group =~ /^\d+$/ && $target_group > 0
    or die "invalid anchored process group";
POSIX::setpgid(0, $target_group) == 0 or die "join anchored process group: $!";
print {$group} "ready\n" or die "write process-group readiness: $!";
my $release = '';
sysread($group_ack, $release, 1) == 1 && $release eq 'R'
    or die "read target-execution release: $!";
close($group_ack) or die "close process-group acknowledgement: $!";
close($group) or die "close process-group receipt: $!";
POSIX::dup2($target_stderr_fd, 2) == 2 or die "restore target stderr: $!";
POSIX::close($target_stderr_fd) == 0 or die "close target stderr descriptor: $!";
open(my $working_directory, '<&=', $working_directory_fd) or die "open working-directory descriptor: $!";
chdir($working_directory) or die "chdir working-directory descriptor: $!";
my $executable = shift @ARGV;
my $logical_program = shift @ARGV;
my $environment_count = shift @ARGV;
defined($environment_count) && $environment_count =~ /^\d+$/
    or die "invalid target environment count";
my %target_environment;
for (1 .. $environment_count) {
    my $name = shift @ARGV;
    my $value = shift @ARGV;
    defined($name) && defined($value) or die "truncated target environment";
    !exists($target_environment{$name}) or die "duplicate target environment key: $name";
    $target_environment{$name} = $value;
}
%ENV = %target_environment;
exec {$executable} $logical_program, @ARGV or die "exec $executable: $!";
"#;

pub(super) fn validate_target_environment(
    environment: &BTreeMap<String, String>,
) -> Result<(), Box<dyn Error>> {
    const RESERVED: &[&str] = &[
        RESOURCE_FD_ENV,
        TARGET_GROUP_ACK_FD_ENV,
        TARGET_GROUP_FD_ENV,
        TARGET_GROUP_ID_ENV,
        TARGET_LIFETIME_LEASE_FD_ENV,
        "RAFTER_INVARIANT_TARGET_STDERR_FD",
        INHERITED_FD_MAX_ENV,
        INHERITED_FDS_ENV,
        WORKING_DIRECTORY_FD_ENV,
    ];
    if let Some(name) = RESERVED
        .iter()
        .find(|name| environment.contains_key(**name))
    {
        return Err(format!("target environment uses reserved launcher key {name}").into());
    }
    Ok(())
}

pub(super) fn target_environment(
    environment: &BTreeMap<String, String>,
    lifetime_descriptor: i32,
) -> Cow<'_, BTreeMap<String, String>> {
    #[cfg(test)]
    if EXPOSE_NEXT_TARGET_LIFETIME_LEASE.with(|expose| expose.replace(false)) {
        let mut environment = environment.clone();
        environment.insert(
            TEST_TARGET_LIFETIME_LEASE_FD_ENV.to_owned(),
            lifetime_descriptor.to_string(),
        );
        return Cow::Owned(environment);
    }
    let _ = lifetime_descriptor;
    Cow::Borrowed(environment)
}

#[cfg(test)]
pub(crate) fn expose_next_target_lifetime_lease() {
    EXPOSE_NEXT_TARGET_LIFETIME_LEASE.with(|expose| expose.set(true));
}

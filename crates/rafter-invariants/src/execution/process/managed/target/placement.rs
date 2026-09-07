//! Target placement transitions from wrapper group to anchored process group.
//!
//! Each transition is only legal from one prior placement, so a repeated
//! publication, an out-of-order promotion, or a launcher that never published
//! is rejected rather than silently accepted.

use super::{ManagedProcess, TargetPlacement};

impl ManagedProcess {
    pub(crate) fn promote_target_group(
        &mut self,
        process_group: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !self.target.is_owned() {
            return Err("target process-group anchor was already released".into());
        }
        if self.target.id() != process_group {
            return Err(format!(
                "target process group {process_group} does not match owned anchor group {}",
                self.target.id()
            )
            .into());
        }
        let TargetPlacement::JoiningAnchorGroup { launcher } = self.placement else {
            return Err(format!(
                "target process group became ready from invalid placement {:?}",
                self.placement
            )
            .into());
        };
        self.placement = TargetPlacement::InAnchorGroup { launcher };
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn set_target_group(
        &mut self,
        process_group: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if process_group != self.target.id() {
            return Err("test target process group does not match its anchor".into());
        }
        self.placement = TargetPlacement::InAnchorGroup {
            launcher: self.wrapper.id(),
        };
        Ok(())
    }

    pub(crate) fn record_published_target(
        &mut self,
        launcher: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.placement != TargetPlacement::UnpublishedInWrapperGroup {
            return Err(format!(
                "target launcher publication repeated from {:?}",
                self.placement
            )
            .into());
        }
        self.placement = TargetPlacement::PublishedInWrapperGroup { launcher };
        Ok(())
    }

    pub(crate) fn begin_target_group_transition(
        &mut self,
        launcher: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.placement != (TargetPlacement::PublishedInWrapperGroup { launcher }) {
            return Err(format!(
                "target launcher {launcher} began anchor transition from {:?}",
                self.placement
            )
            .into());
        }
        self.placement = TargetPlacement::JoiningAnchorGroup { launcher };
        Ok(())
    }
}

//! How a refusal reads, which is part of the artifact rather than a debug aid.

use crate::{
    store::{LockStoreError, SlotDamage, SlotIndex},
    FencingToken, ResourceName,
};

/// The two errors that name a mark regression must each read as one
/// sentence.
///
/// They share a clause and supply their own subjects, and the shared part
/// used to be a whole sentence — so the composed one read "…adopting
/// lock-state.0 in its place would resource orders/shard-0 would drop from
/// fencing high-water mark 2 to 1". This is a string comparison rather than
/// a variant match because the defect was entirely in the rendering: every
/// field was right and the sentence was not.
#[test]
fn renders_a_mark_regression_as_one_sentence_after_either_subject() {
    let resource = ResourceName::new("orders/shard-0").expect("the name is admissible");
    let acknowledged = FencingToken::new(2).expect("token two is non-zero");
    let offered = FencingToken::new(1).expect("token one is non-zero");

    assert_eq!(
        LockStoreError::MarkRegression {
            resource,
            acknowledged,
            offered: Some(offered),
        }
        .to_string(),
        "the state offered would drop resource orders/shard-0 from fencing \
         high-water mark 2 to 1"
    );
    assert_eq!(
        LockStoreError::MarkRegression {
            resource,
            acknowledged,
            offered: None,
        }
        .to_string(),
        "the state offered would lose resource orders/shard-0's fencing \
         high-water mark of 2"
    );
    assert_eq!(
        LockStoreError::DiscardWouldRegressMark {
            slot: SlotIndex::One,
            damage: SlotDamage::UnsealedCompleteImage {
                len: 180,
                generation: 4,
            },
            adopted: SlotIndex::Zero,
            resource,
            acknowledged,
            offered: Some(offered),
        }
        .to_string(),
        "giving up lock-state.1, which holds a whole 180 byte image of generation 4 \
         whose publication mark reads unsealed, and adopting lock-state.0 in its \
         place would drop resource orders/shard-0 from fencing high-water mark 2 to 1"
    );
}

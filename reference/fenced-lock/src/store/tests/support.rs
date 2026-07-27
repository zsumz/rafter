//! Fixtures the byte-level checks are written against.

use rafter::LogIndex;

use crate::{
    store::image::encode_image, ClientId, Command, LeaseDuration, LockConfig, LockService,
    Operation, RequestFingerprint, RequestIdentity, ResourceName, Sequence, SessionEpoch,
};

/// One sealed image over a lock service holding `resources` tenures.
///
/// An empty service would exercise the header and almost none of the
/// payload, and the invariants below are about *every* byte of an image.
/// The resource count is a parameter so a test can build two images of
/// different lengths, which is the shape a shorter publication over a longer
/// one leaves behind.
pub(super) fn sealed_image_of(resources: u32, generation: u64) -> Vec<u8> {
    let config = LockConfig::new(2, 8).expect("bounds are non-zero");
    let mut service = LockService::new(config);
    let client_id = ClientId::new(0);
    let session_epoch = SessionEpoch::new(1).expect("epoch one is valid");
    service.apply(Command::OpenSession {
        client_id,
        session_epoch,
    });
    for index in 0..resources {
        let operation = Operation::Acquire {
            resource: ResourceName::new(&format!("orders/shard-{index}"))
                .expect("the name is legal"),
            lease: LeaseDuration::new(10).expect("a lease is non-zero"),
        };
        service.apply(Command::Submit {
            request: RequestIdentity {
                client_id,
                session_epoch,
                sequence: Sequence::new(u64::from(index) + 1).expect("sequences start at one"),
                fingerprint: RequestFingerprint::of(&operation),
            },
            operation,
        });
    }
    encode_image(
        config,
        &service,
        LogIndex(u64::from(resources) + 1),
        generation,
    )
    .expect("the image encodes")
}

pub(super) fn sealed_image() -> Vec<u8> {
    sealed_image_of(1, 1)
}

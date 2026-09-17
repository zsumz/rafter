//! State-floor spelling compatibility tests.

use serde::Deserialize;

use super::{state_floors, SimulatorStateFloors};

#[derive(Deserialize)]
struct Fixture {
    #[serde(deserialize_with = "state_floors")]
    state_floors: SimulatorStateFloors,
}

#[test]
fn shared_historical_floor_format_remains_supported() {
    let fixture: Fixture = serde_json::from_value(serde_json::json!({
        "state_floors": "13000000-protocol-and-verifier"
    }))
    .expect("historical shared floor deserializes");
    assert_eq!(
        fixture.state_floors,
        SimulatorStateFloors::Aggregate {
            protocol: 13_000_000,
            verifier: 13_000_000,
        }
    );
}

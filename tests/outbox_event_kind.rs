use obix::OutboxEventKind;
use obix::out::OutboxEventKind as Kind;
use serde::{Deserialize, Serialize};

// A leaf event enum: variants are classified in place.
#[derive(Debug, PartialEq, Serialize, Deserialize, OutboxEventKind)]
#[serde(tag = "type")]
enum PriceEvent {
    #[obix(ephemeral)]
    PriceUpdated {
        usd: f64,
    },
    #[obix(ephemeral)]
    ProviderPriceFetched {
        provider: String,
        usd: f64,
    },
    ClosingRateCaptured {
        usd: f64,
    },
}

#[test]
fn leaf_classification() {
    assert_eq!(
        PriceEvent::EPHEMERAL_VARIANTS,
        &["PriceUpdated", "ProviderPriceFetched"],
    );
    assert!(Kind::is_ephemeral(&PriceEvent::PriceUpdated { usd: 1.0 }));
    assert!(Kind::is_ephemeral(&PriceEvent::ProviderPriceFetched {
        provider: "bitfinex".to_string(),
        usd: 1.0,
    }));
    assert!(!Kind::is_ephemeral(&PriceEvent::ClosingRateCaptured {
        usd: 1.0
    }));
    assert_eq!(
        <PriceEvent as Kind>::ephemeral_event_types(),
        vec![("PriceUpdated", "*"), ("ProviderPriceFetched", "*")],
    );
}

// An aggregate: unmarked single-field variants delegate to the inner enum;
// a marked variant is ephemeral as a whole, inner enum or not.
#[derive(Debug, PartialEq, Serialize, Deserialize, OutboxEventKind)]
#[serde(tag = "module")]
enum BankEvent {
    Price(PriceEvent),
    Governance(GovernanceEvent),
    #[obix(ephemeral)]
    DomainConfig(DomainConfigEvent),
}

// A bare derive classifies every variant as persistent.
#[derive(Debug, PartialEq, Serialize, Deserialize, OutboxEventKind)]
#[serde(tag = "type")]
enum GovernanceEvent {
    PolicyEnacted,
}

// No OutboxEventKind impl needed: the wrapping variant is marked ephemeral.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct DomainConfigEvent;

#[test]
fn aggregate_delegates_and_folds() {
    assert!(Kind::is_ephemeral(&BankEvent::Price(
        PriceEvent::PriceUpdated { usd: 1.0 }
    )));
    assert!(!Kind::is_ephemeral(&BankEvent::Price(
        PriceEvent::ClosingRateCaptured { usd: 1.0 },
    )));
    assert!(!Kind::is_ephemeral(&BankEvent::Governance(
        GovernanceEvent::PolicyEnacted
    )));
    assert!(Kind::is_ephemeral(&BankEvent::DomainConfig(
        DomainConfigEvent
    )));

    // Own-level marks only: the folded inner classifications are reported by
    // `ephemeral_event_types`, not duplicated into EPHEMERAL_VARIANTS.
    assert_eq!(BankEvent::EPHEMERAL_VARIANTS, &["DomainConfig"]);
    assert_eq!(
        <BankEvent as Kind>::ephemeral_event_types(),
        vec![
            ("Price", "PriceUpdated"),
            ("Price", "ProviderPriceFetched"),
            ("DomainConfig", "*"),
        ],
    );
}

// Tags must be the values serde puts on the wire, so that registry consumers
// (e.g. JSON Schema filtering) can match them against `oneOf` discriminants.
#[derive(Debug, PartialEq, Serialize, Deserialize, OutboxEventKind)]
#[serde(tag = "type", rename_all = "camelCase")]
enum CamelEvent {
    #[obix(ephemeral)]
    PriceUpdated {
        usd: f64,
    },
    #[obix(ephemeral)]
    #[serde(rename = "provider-price")]
    ProviderPriceFetched {
        usd: f64,
    },
    ClosingRateCaptured {
        usd: f64,
    },
}

#[test]
fn tags_follow_serde_renaming() {
    assert_eq!(
        CamelEvent::EPHEMERAL_VARIANTS,
        &["priceUpdated", "provider-price"]
    );
    // Cross-check against serde's own discriminants.
    assert_eq!(
        serde_json::to_value(CamelEvent::PriceUpdated { usd: 1.0 })
            .unwrap()
            .get("type")
            .unwrap(),
        &serde_json::json!("priceUpdated"),
    );
    assert_eq!(
        serde_json::to_value(CamelEvent::ProviderPriceFetched { usd: 1.0 })
            .unwrap()
            .get("type")
            .unwrap(),
        &serde_json::json!("provider-price"),
    );
    assert_eq!(
        serde_json::to_value(BankEvent::DomainConfig(DomainConfigEvent))
            .unwrap()
            .get("module")
            .unwrap(),
        &serde_json::json!("DomainConfig"),
    );
}

// `#[serde(other)]` is a catch-all for unknown wire tags; it can never be
// ephemeral (the derive rejects that combination) and classifies persistent.
#[derive(Debug, PartialEq, Serialize, Deserialize, OutboxEventKind)]
#[serde(tag = "type")]
enum VersionedEvent {
    #[obix(ephemeral)]
    Ping,
    Pong,
    #[serde(other)]
    Unknown,
}

#[test]
fn serde_other_is_persistent() {
    assert!(Kind::is_ephemeral(&VersionedEvent::Ping));
    assert!(!Kind::is_ephemeral(&VersionedEvent::Unknown));
    assert_eq!(VersionedEvent::EPHEMERAL_VARIANTS, &["Ping"]);
}

// Unit variants and multi-field tuple variants are leaves: classification
// comes from the marker alone, with no delegation. (Serde is not required
// for classification, and internally-tagged serde enums do not allow tuple
// variants — so this enum deliberately derives nothing but the kind.)
#[derive(Debug, PartialEq, OutboxEventKind)]
enum MixedShapesEvent {
    #[obix(ephemeral)]
    Poke,
    Quiet,
    #[obix(ephemeral)]
    Pair(&'static str, u32),
}

#[test]
fn unit_and_multi_field_variants_are_leaves() {
    assert!(Kind::is_ephemeral(&MixedShapesEvent::Poke));
    assert!(!Kind::is_ephemeral(&MixedShapesEvent::Quiet));
    assert!(Kind::is_ephemeral(&MixedShapesEvent::Pair("a", 1)));
    assert_eq!(
        <MixedShapesEvent as Kind>::ephemeral_event_types(),
        vec![("Poke", "*"), ("Pair", "*")],
    );
}

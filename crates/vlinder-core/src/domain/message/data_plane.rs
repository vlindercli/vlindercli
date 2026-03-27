//! `ObservableMessageV2` — clean message types with routing separated from payload.

use super::super::routing_key::DataRoutingKey;
use super::complete::CompleteMessage;
use super::invoke::InvokeMessage;
use super::request::RequestMessage;
use super::response::ResponseMessage;

/// Observable message with routing and payload cleanly separated.
///
/// New message types are added here as they migrate from `ObservableMessage`.
/// Old variants are removed from `ObservableMessage` once fully migrated.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ObservableMessageV2 {
    InvokeV2 {
        key: DataRoutingKey,
        msg: InvokeMessage,
    },
    CompleteV2 {
        key: DataRoutingKey,
        msg: CompleteMessage,
    },
    RequestV2 {
        key: DataRoutingKey,
        msg: RequestMessage,
    },
    ResponseV2 {
        key: DataRoutingKey,
        msg: ResponseMessage,
    },
}

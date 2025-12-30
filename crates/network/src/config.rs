use citrea_common::GossipsubConfig;
use libp2p::gossipsub;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

pub(crate) fn build_gossipsub_config(gossipsub_config: &GossipsubConfig) -> anyhow::Result<gossipsub::Config> {
    let GossipsubConfig {
        heartbeat_interval_secs,
    } = gossipsub_config;

    let heartbeat_interval = Duration::from_secs(*heartbeat_interval_secs);

    let message_id_fn = |message: &gossipsub::Message| {
        let mut s = DefaultHasher::new();
        message.data.hash(&mut s);
        gossipsub::MessageId::from(s.finish().to_string())
    };
    // Set a custom gossipsub configuration
    let config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(heartbeat_interval) // This is set to aid debugging by not cluttering the log space
        .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message
        // signing)
        .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated.
        .build()?;
    Ok(config)
}
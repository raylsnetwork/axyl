use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::profile::NetworkProfile;

/// A client's network configuration file: any number of named subnets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfigFile {
    /// The subnets this client operates, keyed by name.
    pub networks: BTreeMap<String, NetworkProfile>,
}

impl NetworkConfigFile {
    /// Look up a subnet by name.
    pub fn subnet(&self, name: &str) -> Option<&NetworkProfile> {
        self.networks.get(name)
    }
}

//! Tests that the active network profile (set via --config-file) propagates
//! into the chain spec built by RethEnv.
//!
//! This test runs in a separate binary from the unit tests in
//! `rayls-execution-evm` because `set_active_profile` writes to a process-
//! wide `OnceLock`; keeping it isolated prevents contamination of other
//! test binaries.

use rayls_execution_evm::{
    network_profile::{set_active_profile, NetworkProfile},
    reth_env::RethEnv,
    RaylsHardFork, RaylsHardforks,
};
use rayls_infrastructure_types::TaskManager;

#[tokio::test]
async fn active_profile_schedule_reaches_chain_spec() -> eyre::Result<()> {
    let _guard = e2e_tests::IT_TEST_MUTEX.lock();

    let profile: NetworkProfile = serde_yaml::from_str(
        r#"
chain_id: 7295799
hardforks:
  TransactionLoadBalancing: 777777
"#,
    )?;
    set_active_profile(profile)?;

    let chain = rayls_infrastructure_types::test_chain_spec_arc();
    let tmp_dir = tempfile::TempDir::new()?;
    let task_manager = TaskManager::new("Test Task Manager");
    let reth_env = RethEnv::new_for_temp_chain(chain, tmp_dir.path(), &task_manager, None).await?;

    let spec = reth_env.rayls_chain_spec();
    assert_eq!(
        spec.rayls_fork_activation(RaylsHardFork::TransactionLoadBalancing),
        rayls_execution_evm::ForkCondition::Block(777777)
    );
    // A fork absent from the file's schedule stays `Never`.
    assert_eq!(
        spec.rayls_fork_activation(RaylsHardFork::Eip1559),
        rayls_execution_evm::ForkCondition::Never
    );
    Ok(())
}

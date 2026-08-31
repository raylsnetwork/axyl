//! Execution layer for worker and primary roles.
//!
//! The worker components track the canonical tip to build batches and validate peers' batches; the
//! primary's engine executes consensus output and extends the canonical tip. [`ExecutionNode`] is
//! the thread-safe wrapper around the inner type holding the logic.

mod node;
mod node_builder;
mod node_inner;
mod rayls_builder;
mod txn_forwarder;

pub use node::*;
pub use rayls_builder::*;
pub use rayls_execution_evm::worker::*;

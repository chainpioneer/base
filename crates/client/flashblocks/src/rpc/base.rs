//! Base namespace RPC trait definitions and implementations.

use alloy_eips::{eip2930::AccessListResult, BlockId};
use alloy_primitives::U256;
use alloy_rpc_types::{
    BlockOverrides,
    state::{StateOverride, StateOverridesBuilder},
};
use jsonrpsee::{
    core::{RpcResult, async_trait},
    proc_macros::rpc,
};
use op_alloy_network::Optimism;
use op_alloy_rpc_types::OpTransactionRequest;
use reth_evm::env::BlockEnvironment;
use reth_rpc_eth_api::helpers::{EthCall, FullEthApi, LoadState};
use tracing::debug;

use crate::{FlashblocksAPI, PendingBlocksAPI};

use super::eth::EthApiExt;

/// Base namespace API for enhanced RPC methods.
#[cfg_attr(not(test), rpc(server, namespace = "base"))]
#[cfg_attr(test, rpc(server, client, namespace = "base"))]
pub trait BaseApi {
    /// Creates an access list and returns gas used with the access list applied.
    ///
    /// Like `eth_createAccessList` but with `blockOverrides` support like `eth_call`.
    /// The returned `gas_used` is from execution with the access list applied.
    #[method(name = "createAccessList")]
    async fn create_access_list(
        &self,
        transaction: OpTransactionRequest,
        block_number: Option<BlockId>,
        state_overrides: Option<StateOverride>,
        block_overrides: Option<Box<BlockOverrides>>,
    ) -> RpcResult<AccessListResult>;
}

#[async_trait]
impl<Eth, FB> BaseApiServer for EthApiExt<Eth, FB>
where
    Eth: FullEthApi<NetworkTypes = Optimism> + Send + Sync + 'static,
    FB: FlashblocksAPI + Send + Sync + 'static,
    jsonrpsee_types::error::ErrorObject<'static>: From<Eth::Error>,
{
    async fn create_access_list(
        &self,
        transaction: OpTransactionRequest,
        block_number: Option<BlockId>,
        state_overrides: Option<StateOverride>,
        block_overrides: Option<Box<BlockOverrides>>,
    ) -> RpcResult<AccessListResult> {
        debug!(
            message = "rpc::create_access_list",
            block_number = ?block_number,
        );

        // Handle flashblocks pending state
        let mut block_id = block_number.unwrap_or_default();
        let mut pending_state = None;
        if block_id.is_pending() {
            self.metrics().rpc_base_create_access_list.increment(1);
            let pending_blocks = self.flashblocks_state().get_pending_blocks();
            block_id = pending_blocks.get_canonical_block_number().into();
            pending_state = pending_blocks.get_state_overrides();
        }

        // Merge state overrides (flashblocks pending + user overrides)
        let mut state_overrides_builder =
            StateOverridesBuilder::new(pending_state.unwrap_or_default());
        state_overrides_builder =
            state_overrides_builder.extend(state_overrides.unwrap_or_default());
        let final_overrides = state_overrides_builder.build();

        // Get evm_env at the resolved block
        let eth_api = self.eth_api();
        let (mut evm_env, at) =
            LoadState::evm_env_at(eth_api, block_id).await.map_err(Into::into)?;

        // Apply block overrides to evm block environment
        if let Some(ref overrides) = block_overrides {
            let block_env = evm_env.block_env.inner_mut();
            if let Some(number) = overrides.number {
                block_env.number = number;
            }
            if let Some(difficulty) = overrides.difficulty {
                block_env.difficulty = difficulty;
            }
            if let Some(time) = overrides.time {
                block_env.timestamp = U256::from(time);
            }
            if let Some(gas_limit) = overrides.gas_limit {
                block_env.gas_limit = gas_limit;
            }
            if let Some(coinbase) = overrides.coinbase {
                block_env.beneficiary = coinbase;
            }
            if let Some(random) = overrides.random {
                block_env.prevrandao = Some(random);
            }
            if let Some(base_fee) = overrides.base_fee {
                block_env.basefee = base_fee.to();
            }
        }

        // Create access list and get gas_used with the access list applied.
        // Internally runs the tx twice: once to discover the access list,
        // once with it applied to get accurate gas_used.
        EthCall::create_access_list_with(
            eth_api,
            evm_env,
            at,
            transaction,
            Some(final_overrides),
        )
        .await
        .map_err(Into::into)
    }
}

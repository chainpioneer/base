//! Base namespace RPC trait definitions and implementations.

use std::future::Future;

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
use reth_evm::{EvmEnvFor, env::BlockEnvironment};
use reth_rpc_eth_api::helpers::{EthCall, FullEthApi, LoadState};
use tracing::debug;

use crate::{FlashblocksAPI, PendingBlocksAPI};

use super::eth::EthApiExt;

/// Base namespace API for enhanced RPC methods.
#[cfg_attr(not(test), rpc(server, namespace = "base"))]
#[cfg_attr(test, rpc(server, client, namespace = "base"))]
pub trait BaseApi {
    /// Creates an access list and estimates gas using the generated access list.
    ///
    /// Combines `eth_createAccessList` and `eth_estimateGas` into a single call,
    /// with support for `blockOverrides` like `eth_call`.
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

        // Step 1: Create access list with block-overrides-modified evm_env
        let acl_result = EthCall::create_access_list_with(
            eth_api,
            evm_env.clone(),
            at,
            transaction.clone(),
            Some(final_overrides.clone()),
        )
        .await
        .map_err(Into::into)?;

        // If access list creation failed, return early with the error
        if acl_result.error.is_some() {
            return Ok(acl_result);
        }

        // Step 2: Set access list on the transaction and estimate gas
        let mut tx_with_acl = transaction;
        tx_with_acl.as_mut().access_list = Some(acl_result.access_list.clone());

        // Run gas estimation in a blocking context
        let gas = spawn_estimate_gas(
            eth_api,
            evm_env,
            tx_with_acl,
            at,
            Some(final_overrides),
        )
        .await
        .map_err(Into::into)?;

        Ok(AccessListResult {
            access_list: acl_result.access_list,
            gas_used: gas,
            error: None,
        })
    }
}

/// Estimates gas with a pre-built EVM environment in a blocking IO context.
///
/// Extracted as a free function returning `impl Future` to avoid higher-ranked
/// lifetime errors that occur when `spawn_blocking_io_fut` is called inside
/// an `#[async_trait]` method (same pattern as `EstimateCall::estimate_gas_at`).
fn spawn_estimate_gas<Eth>(
    eth_api: &Eth,
    evm_env: EvmEnvFor<Eth::Evm>,
    request: OpTransactionRequest,
    at: BlockId,
    state_override: Option<StateOverride>,
) -> impl Future<Output = Result<U256, Eth::Error>> + Send + '_
where
    Eth: FullEthApi<NetworkTypes = Optimism> + Send + Sync + 'static,
{
    eth_api.spawn_blocking_io_fut(move |this| async move {
        let state = this.state_at_block_id(at).await?;
        this.estimate_gas_with(evm_env, request, state, state_override)
    })
}

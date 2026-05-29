//! Base namespace RPC trait definitions and implementations.

use std::future::Future;

use alloy_eips::{BlockId, eip2930::AccessListResult};
use alloy_primitives::U256;
use alloy_rpc_types::{
    BlockOverrides,
    state::{StateOverride, StateOverridesBuilder},
};
use base_common_network::Base;
use base_common_rpc_types::BaseTransactionRequest;
use jsonrpsee::{
    core::{RpcResult, async_trait},
    proc_macros::rpc,
};
use reth_evm::{EvmEnvFor, env::BlockEnvironment};
use reth_rpc_eth_api::helpers::{EthCall, FullEthApi, LoadState};
use tracing::debug;

use super::eth::EthApiExt;
use crate::{FlashblocksAPI, PendingBlocksAPI, metrics::Metrics};

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
        transaction: BaseTransactionRequest,
        block_number: Option<BlockId>,
        state_overrides: Option<StateOverride>,
        block_overrides: Option<Box<BlockOverrides>>,
    ) -> RpcResult<AccessListResult>;
}

impl<Eth, FB> EthApiExt<Eth, FB>
where
    Eth: FullEthApi<NetworkTypes = Base> + Send + Sync + 'static,
{
    /// Estimates gas with a pre-built EVM environment in a blocking IO context.
    ///
    /// Returns `impl Future` and lives outside the `#[async_trait]` impl to avoid the
    /// higher-ranked lifetime errors that occur when `spawn_blocking_io_fut` is invoked
    /// inside an `#[async_trait]` method (the same pattern reth uses for
    /// `EstimateCall::estimate_gas_at`).
    fn spawn_estimate_gas(
        &self,
        evm_env: EvmEnvFor<Eth::Evm>,
        request: BaseTransactionRequest,
        at: BlockId,
        state_override: Option<StateOverride>,
    ) -> impl Future<Output = Result<U256, Eth::Error>> + Send + '_ {
        self.eth_api().spawn_blocking_io_fut(move |this| async move {
            let state = this.state_at_block_id(at).await?;
            this.estimate_gas_with(evm_env, request, state, state_override)
        })
    }
}

#[async_trait]
impl<Eth, FB> BaseApiServer for EthApiExt<Eth, FB>
where
    Eth: FullEthApi<NetworkTypes = Base> + Send + Sync + 'static,
    FB: FlashblocksAPI + Send + Sync + 'static,
    jsonrpsee_types::error::ErrorObject<'static>: From<Eth::Error>,
{
    async fn create_access_list(
        &self,
        transaction: BaseTransactionRequest,
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
            Metrics::rpc_base_create_access_list().increment(1);
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

        // Apply block overrides to the EVM block environment. Mirrors the scalar-field
        // handling of `alloy_evm::overrides::apply_block_overrides`; block-hash overrides
        // are not supported here because the access-list helper builds its own state db
        // internally and cannot be threaded the per-hash overrides.
        if let Some(overrides) = block_overrides.as_deref() {
            let block_env = evm_env.block_env.inner_mut();
            if let Some(number) = overrides.number {
                block_env.number = number.saturating_to();
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
                block_env.basefee = base_fee.saturating_to();
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

        // Step 2: Set the access list on the transaction and estimate gas
        let tx_with_acl = transaction.access_list(acl_result.access_list.clone());

        // Run gas estimation in a blocking context
        let gas = self
            .spawn_estimate_gas(evm_env, tx_with_acl, at, Some(final_overrides))
            .await
            .map_err(Into::into)?;

        Ok(AccessListResult { access_list: acl_result.access_list, gas_used: gas, error: None })
    }
}

# Pythia:

This is a fork of the Aptos-Core Repository:
- [Aptos Core Repository](https://github.com/aptos-labs/aptos-core)
- [Aptos Labs](https://aptoslabs.com/)


The workload files can be found under:
- [Solana/Mixed](aptos-move/e2e-tests/src/solana_distribution.rs)
- [P2P/NFT](aptos-move/e2e-tests/src/account_activity_distribution.rs)
- [DEX](aptos-move/e2e-tests/src/uniswap_distribution.rs)

The benchmark is invoked in: [Run_Benchmark](aptos-move/e2e-testsuite/tests/run_benchmark.rs)
and the changes to the scheduler can be found in: [Scheduler](aptos-move/block-executor/src/scheduler.rs) and [Executor](aptos-move/block-executor/src/executor.rs)

To run the benchmark enter [Benchmark folder](aptos-move/e2e-testsuite) and run: "cargo test main --release | grep "#" > output.txt"
Make sure to get the results from both the Pythia-Benchmark branch as well as the BlockSTM-Benchmark branch
 


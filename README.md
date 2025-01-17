# Anthemius[BlockSTM]:

This is a fork of the Aptos-Core Repository:
- [Aptos Core Repository](https://github.com/aptos-labs/aptos-core)
- [Aptos Labs](https://aptoslabs.com/)


The benchmark is invoked in: [Run_Benchmark](aptos-move/e2e-testsuite/tests/run_benchmark.rs) (where also the batch handler is situated)
and the batch scheduler can be found here: [Batch Scheduler](mempool/src/core_mempool/filler.rs)

To run the benchmark enter [Benchmark folder](aptos-move/e2e-testsuite) and run: "cargo test main --release | grep "#" > output.txt"
Make sure to get the results from both the Anthemius_Chiron branch as well as the Anthemius_BlockSTM branch
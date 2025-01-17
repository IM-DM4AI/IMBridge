# What is IMBridge

IMBridge is a prototype system based on OceanBase 4.3. It is designed to mitigate impedence mismatches between database engines and prediction query execution.

## Impedence Mismatches

* Repetitive Inference Context Setup
* Undesirable Batching Inference

## Solutions

* Automatic Inference Context Reuse Cache
* Batch-aware Function Invocation

## System Implementation
mainly in 'src/sql/engine/python_udf_engine'
* Zero-copy Vectorized Python UDF based prediction function evaluation
* One-off Inference Context Setup
* Python UDF metadata management (Parser, Resolver, RootService...)
* Statement Rewriter to detect and extract Python UDF
* Specialised Prediction-aware Operator with Batch Control and Inference Context Reuse Cache.
* Adaptive inference batch size esitimated using a heuristic algorithm

## Quick Start

1. Setup the system environment. (compile Python 3.12 from source code to target path, see cmake/PythonEnv.cmake, other compilation details see /docs)
2. Download and install OceanBase-all-in-one package (Version 4.3.0.1) from www.oceanbase.com/softwarecenter.
3. Deploy the OceanBase cluster.
4. Config and run replaceOb.sh shell. (./replaceOb.sh debug/release)
5. Redeploy the OceanBase cluster.
6. Connect to the database, create Python UDFs and make prediction queries.
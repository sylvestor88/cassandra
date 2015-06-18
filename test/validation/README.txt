Cassandra Validation Harness v0.2
=================================

Updated 2015-6-18

About
-----

The Cassandra Validation Harness is a test running tool designed to simulate
a production cluster and environment. Real workloads will be running concurrently
alongside administrative operations, allowing us to test if the interaction
of multiple features produces novel bugs.

Architecture
------------

CVH exists as a set of test modules. Each module should extend o.a.c.modules.Module.
Each module is wholly isolated, and is responsible for performing its own
operations and validation.

CVH interacts with Cassandra via a Bridge. Currently, we have only implemented
CCMBridge, to run clusters using the Cassandra Cluster Manager. In the future,
we will add Bridges that will run CVH against real clusters where nodes are on
different hosts.

Tests are defined as yaml files located in test/validation/o.a.c/htest/. Each
yaml file defines a number of modules to run concurrently, as well as options
for the modules, and cassandra.yaml options. For more details on how to format
a yaml, please see the README in the htest package directory.

Tests can be run via the ant target `ant htest`
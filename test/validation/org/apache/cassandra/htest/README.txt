Updated 2015-6-18

Test yaml's support the following options:

nodeCount: Number of nodes in the cluster. Only support one DC currently.

modules: A list of which modules to run in this test.

cassandrayaml: A map of options to forward directly to the cassandra.yaml of each node in the test cluster

moduleArgs: A map of maps. The keys of the outer map correspond to modules in your test. The inner maps correspond to settings for those modules. Each module will have its own settings to choose from, and not all modules will have settings. Please refer to the documentation or code for each module to find what is supported.

The yaml syntax for a list is:

key:
    - value1
    - value2

The yaml syntax for a map is:

key: !!map
    value

The yaml syntax for a nested map is:

key: !!map
    inner_key: !!map
        inner_value
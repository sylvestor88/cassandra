# CTool Bridge

## Prerequisites:

* ctool should be installed and running on the system from where you are running the tests.

  ctool Installation: Refer the following link for ctool installation details [on Github](https://github.com/riptano/automaton)

* `ctool_launch.py` should be present in o.a.c.bridges.ctoolbridge package. It can be downloaded from [on Github](https://github.com/riptano/automaton/  blob/master/cstar_perf/ctool_launch.py)

* `cluster.json` should be present in o.a.c.bridges.ctoolbridge package with the below given configuraton.

  ```sh
  {
    "ctool_options": {
      "existing_cluster": true,
      "stripe": false,
      "name": "validation",
      "bootstrap_controller": true
    }
  }
  ```
## How to Run the Harness with CToolBridge:

1. Each test in the Harness can have a set of test modules. You may write as many modules you want in o.a.c.modules package.
   Every module should extend o.a.c.modules.Module  

2. Make sure that the cluster is instantiated with CToolBridge in o.a.c.HarnessTest

  ```sh
  @Test
    public void harness()
    {
        ........
        ........
        ........
        cluster = new CToolBridge(config);
        ........
        ........
        ........
    }
  ```
3. You can now configure your test in .yaml file in o.a.c.htest package. The test configuration takes following parameters:

    * nodeCount: Ineteger value that specifies the number of nodes you want in your cluster.
    * modules: Modules can be configured in groups with each group having one or more modules i.e. name of the modules. You can configure any number 
       of module groups.
    * cassandrayaml: You can specify parameters for cassandra conf that gets configured to you cassandra installation.
    * moduleArgs: Allows to pass arguments for a specific module
    * ignoredErrors:
    * requiredErrors:

   Below is the sample of a test file named mytest.yaml
    ```sh  
       nodeCount: 3
       modules:
           - [ArbitraryStressOperationModule]
           - [SimpleWriteModule, SimpleCompactModule]
       cassandrayaml: !!map
           concurrent_writes: 64
       moduleArgs: !!map
           ArbitraryStressOperationModule: !!map
               stress_settings: write n=100   
    ```

4. You may now run HarnessTest which will run all the .yaml files in o.a.c.htest package as individual tests


## Accessing tools from within test modules

  The CToolBridge provides access to tools such as nodetool, sstablesplit and sstablemetadata. This allows the developers to access these tools from within their modules via bridge.

  You can create a node instance by passing the node number as String. 
  Example: For first node, you will pass "0"
  
  ```sh
  Node myNode = new Node("0");
  ```
  Once, you have created an instance for a particular node, you may easily use the below tools as follows:

  * Nodetool: 
    The nodetool function takes 3 parameters: i) the node instance; ii) nodetool command; iii) arguments(optional)
    In case where no arguments are given, an empty string is passed to the function.

    ```sh
    bridge.nodetool(myNode, "info", "");
    ```  

  * SSTableSplit:
    The sstablesplit function also takes 3 parameters: i) the node instance; ii) options (optional); iii) keyspace_path
    In case where no options are given, an empty string is to be passed to the function. This will split all the sstables in that node with
    the default size of 50 MB.
    keyspace_path takes in the keyspace and the column-family in a definite format.

    sstablesplit without any options:
    ```sh
    bridge.sstablesplit(myNode, "", "k/t");
    ```
    sstablesplit with options:
    ```sh
    bridge.sstablesplit(myNode, "-s 100", "k/t");
    ``` 
    where k is the keyspace,
          t is the column families,
          '-s 100' is the max size of output sstables

  * SSMetaData:
    The sstablemetadata function takes 2 parameters: i) the node instance; ii) keyspace_path

    ```sh
    bridge.sstablesplit(myNode, "k/t");
    ```
    where k is the keyspace,
          t is the column families

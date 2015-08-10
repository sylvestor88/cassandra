# CCM Bridge

## Prerequisites:

* CCM (Cassandra Cluster Manager) should be installed and running on the system from where you are running the test.
              
  CCM Installation:

   ccm uses python distutils so from the source directory run:
   ```sh
   sudo ./setup.py install
   ```    
   ccm is available on the Python Package Index:
   ```sh
   pip install ccm
   ```
   There is also a Homebrew package available:
   ```sh
   brew install ccm
   ```
  refer the following link for details [on Github](https://github.com/pcmanus/ccm.git)
 

## How to Run the Harness with CCMBridge:

1. Each test in the Harness can have a set of test modules. You may write as many modules you want in o.a.c.modules package.
   Every module should extend o.a.c.modules.Module  

2. Make sure that the cluster is instantiated with CCMBridge in o.a.c.HarnessTest

  ```sh
  @Test
    public void harness()
    {
        ........
        ........
        ........
        cluster = new CCMBridge(config);
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

  The CCMBridge provides access to tools such as nodetool, sstablesplit and sstablemetadata. This allows the developers to access these tools from within their modules via bridge.

  You can create a node instance by passing the node number as String. 
  Example: For node 1, you will pass "1"
  
  ```sh
  Node myNode = new Node("1");
  ```
  Once, you have created an instance for a particular node, you may easily use the below tools as follows:

  * Nodetool: 
    The nodetool function takes 3 parameters: i) the node instance; ii) nodetool command; iii) arguments(optional)
    In case where no arguments are given, an empty string is passed to the function.

    ```sh
    bridge.nodetool(myNode, "info", "");
    ```  

  * SSTableSplit:
    The sstablesplit function also takes 3 parameters: i) the node instance; ii) options (optional); iii) keyspace (always an empty string)
    In case where no options are given, an empty string is to be passed to the function. This will split all the sstables in that node with
    the default size of 50 MB.

    sstablesplit without any options:
    ```sh
    bridge.sstablesplit(myNode, "", "");
    ```
    sstablesplit with options:
    ```sh
    bridge.sstablesplit(myNode, "-k k -c t -s 100", "");
    ``` 
    where k is the keyspace,
          t is the column families
          100 is the max size of output sstables

  * SSMetaData:
    The sstablemetadata function takes 2 parameters: i) the node instance; ii) keyspace (optional)
    In case where no keyspace is given, an empty string is to be passed to the function. This will show the metadata of all the sstables
    for that node.

    sstablemetadata without keyspace:
    ```sh
    bridge.sstablesplit(myNode, "");
    ```
    sstablemetadata with keyspace:
    ```sh
    bridge.sstablesplit(myNode, "k");
    ```
    where k is the keyspace

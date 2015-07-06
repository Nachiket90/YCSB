Yahoo! Cloud System Benchmark (YCSB)
====================================
[![Build Status](https://travis-ci.org/brianfrankcooper/YCSB.png?branch=master)](https://travis-ci.org/brianfrankcooper/YCSB)

Links
-----
http://wiki.github.com/brianfrankcooper/YCSB/  
http://research.yahoo.com/Web_Information_Management/YCSB/  
ycsb-users@yahoogroups.com  

Getting Started
---------------

1. Download the latest release of YCSB:

    ```sh
    wget https://github.com/downloads/brianfrankcooper/YCSB/ycsb-0.1.4.tar.gz
    tar xfvz ycsb-0.1.4
    cd ycsb-0.1.4
    ```
    
2. Set up a database to benchmark. There is a README file under each binding 
   directory.

3. Run YCSB command. 
    
    ```sh
    bin/ycsb load basic -P workloads/workloada
    bin/ycsb run basic -P workloads/workloada
    ```

  Running the `ycsb` command without any argument will print the usage. 

  This build contains new workloadg which is designed for benchmarking media data (images/videos) 
  using YCSB. Required steps are same as above with change in workload name,
  
  mkdir YCSB/workload-data ----- All media data should be present in this directory.
  
    ```sh
    bin/ycsb load basic -P workloads/workloadg
    bin/ycsb run basic -P workloads/workloadg
    ```
  
  See https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload
  for a detailed documentation on how to run a workload.

  See https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for 
  the list of available workload properties.

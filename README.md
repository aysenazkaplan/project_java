##############################################

# Programmierpraktikum: Datensysteme WISE 25/26

##############################################

Goal:
------------------
- Create your own index structure and implement multithreaded transaction handling
- Implement the API provided in server.h
    - Every function header contains a detailed description of the expected functionality.
- Additional information for this assignment can be found in  Elizabeth G. Reid's master thesis [1]
  Elizabeth G. Reid, Design and Evaluation of a Benchmark for Main Memory Transaction Processing Systems

Test your implementation:
------------------
- In order to pass this course the `UnitTests` and `SpeedTest`  that can be found in the `test` directory have to pass.

- Write additional unit tests where you also test some special use cases
  (HINT: Take a look at [1])
- Running the `SpeedTest` using the following configuration:
````
  - seed = 1468 
  - NUM_DEADLOCK = 0
  - NUM_TXN_FAIL = 0
  - NUM_TXN_COMP = 0
  - global_time = 0
  - NUM_POP_INSERTS = 4000
  - NUM_TESTS_PER_THREAD = 16000
  
  (Note: these variables can be changed in the first few lines of th SpeedTest)
````

on a correct implementation you should receive an output similar to this where the individual 
execution time may differ depending on your implementation:

````
  speed_test called with 4000 populate inserts per thread and 16000 tests per thread

  Running the Speed Test, seed = 1468
  Creating 29 indices.
  Populating indices 29.
  Time to populate: 37 milliseconds.
  Testing the indices.
  Time to test: 1337 milliseconds.
  Testing complete.
          NUM_DEADLOCK: 0
          NUM_TXN_FAIL: 0
          NUM_TXN_COMP: 800000
  Overall time to run: 1375 milliseconds.

  Cleaning up.
````

- The minimum performance target for your implementation is: T(SUT) < 4 * T(ref_impl)
- To get the benchmark of the reference implementation T(ref_impl) in Linux or OSX please 
complete the following steps in the the `C/C++ project` folder which can be found on the course website.
- You need to run the following commands:
    - cd project/driver
    - if you are on Linux replace -lspeedXXX in the next line with -lspeedLinux, for OSX take -lspeedOSX
    - gcc -L/<FULL_PATH_TO_PROJECT>/bin -pthread -o speed_test_ref speed_test.c -lspeedXXX
    - In order to run ```./speed_test_ref 1468 0 0 0 0 4000 16000``` you need to export your environment variable:
      ```export LD_LIBRARY_PATH=/<FULL_PATH_TO_PROJECT>/bin:$LD_LIBRARY_PATH```
    - Running ```./speed_test_ref 1468 0 0 0 0 4000 16000``` provides T(ref_impl)
- To get the benchmark of the reference implementation T(ref_impl) in Windows, you need to run the following commands:
    - cd project/driver
    - gcc -L/<FULL_PATH_TO_PROJECT>/bin -pthread -o speed_test_ref speed_test.c -llibspeed
        - Add <FULL_PATH_TO_PROJECT>/bin to the PATH variable
    - Running ```./speed_test_ref 1468 0 0 0 0 4000 16000``` provides T(ref_impl)


System specifications:
-----------------
We will test your implementation on our scale-up box:
- CPU: Intel(R) Xeon(R) Gold 6338 CPU @ 2.00GHz
- Cores: 128
- RAM: 1008GB
- OS: Ubuntu 24.04
- Java-17: Openjdk 17.0.16

Make sure that your implementation works with openjdk-17.0.16 and runs on Ubuntu 24.04!


----------------
[1] Elizabeth G. Reid, Design and Evaluation of a Benchmark for Main Memory Transaction Processing Systems,
[Link](https://core.ac.uk/download/pdf/4417736.pdf)
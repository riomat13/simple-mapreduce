# Simple MapReduce

This is a project to build simple *mapreduce*[[1](#ref1)] app with *C++* using *MPI*[[2](#ref2)].
(some APIs namings are referred from *Apache Hadoop*[[link](https://hadoop.apache.org/docs/current/)])

### Disclaimer

Mapreduce built at this project may not be a right structured.
This is built based on paper [[2](#ref1)], but I may take wrong some parts.
Additionally, this is tested with several texts with word counting tasks
but not fully tested, so it may have bugs.

## TOC

1. [Project structure](#1-project-structure)
2. [Setup](#2-setup)
3. [Execute example](#3-execute-example)
4. [Performance](#4-performance)
5. [TODO](#5-todo)
6. [References](#6-references)

## <a name="1-project-structure"></a>1 Project structure

The directory is structured as following.

```
.
├─ app/                  # put main task of mapreduce
|   ├─ CMakeLists.txt    # cmake file for main task
|   ├─ sourcelist.cmake  # put all source file used in the mapreduce task
|   └─ word_count.cc     # example mapreduce task
|
├─ cmake/          # contains file(s) used for build
├─ include/        # directory containing documents and related items
├─ inputs/         # directory to store input files to process
├─ outputs/        # directory to store processed output
|                    (this will be generated if not exist)
├─ src/            # contains all source files used for mapreduce
|
├─ baseline.cc     # baseline script for performance comparison
├─ CMakeLists.txt  # cmake file for library
├─ Dockerfile      # build container to run app
└─ run_docker      # script to run tasks on docker container
```

## <a name="2-setup"></a>2 Setup
### 2.1 Install dependencies

This needs to install `cmake`, `g++ (>=9)` and `mpi`.
(In this project, `OPEN_MPI` is used.)

If use `open-mpi`, run the following command,
```sh
# on Ubuntu
$ sudo apt install g++ cmake openmpi-bin openmpi-common libopenmpi-dev
```

If you use alternatives such as `MPICH`, check their website and install it manually.

### 2.2 Network settings
This project is run with 2 PCs to check performance. The result is described at [section4](#4-performance).

If you want to connect other machines, use `NFS` and set the shared directory, then run from there.
However, this requires additional tasks for network settings or configurations, such as SSH settings. Documentation says MPI uses arbitorary ports so to make it easier, temporarily turn off firewall.
(for OPEN-MPI, see [FAQ](https://www.open-mpi.org/faq/?category=running))

Once conneced machines to the same network, need to set host machines.
The host list should be stored in `./conf/host_file`.
If use the script from the next section to run the operation, the file name and hte location must match.

## <a name="3-execute-example"></a>3 Execution


### 3.1 Initial build

Before starting all tasks, need to build mapreduce library first.
In order to do that, run the following script in shell:
```sh
# at root directory
$ mkdir -p build && cd build
$ cmake ..
$ make -j
```

or, if you want to build both library and executable(see section 3.2),
run the following instead:
```sh
$ mkdir -p build && cd build
$ cmake -DSIMPLEMR_BUILD_APP=ON ..
$ make -j
```

### 3.2 Example task (Word count)

As an example task, the popular preprocessing steps "word count" is adopted,
because it is simple and easy to implement, but good for checking performances.

The tokenizing processes are:

  - Remove punctuations
  - Split by space or tabs

The word counting app itself is located at `./app/word_count.cc`.

Note that, in the app script, relative paths are used
so that it needs to follow the commands below otherwise crash when execute the code.

```sh
$ cd app
$ mkdir -p build && cd build
$ cmake ..
$ make -j
```

then, this will generate `run_task` in root directory,
so change directory to root and execute `run_task` with *MPI*.

```sh
# change directory to root
$ cd ../..
$ mpirun [options] ./run_task

# for instance,
# use all node on the local machine
$ mpirun ./run_task
# use machines set in `host_file` and 4 nodes in total
$ mpirun -np 4 --hostfile host_file ./run_task
```

If you need root privilages to execute `mpirun`, create a user and run with `su -c`.

Note that if the environment has **only 1 core available**, similar to following error will raise:

```
... (some error messages)
--------------------------------------------------------------------------
[ERROR] [2021-01-23 12:34:56.789] No worker node is provieded. Aborting...
--------------------------------------------------------------------------
... (some error messages)
```

The process will be aborted if only 1 core is available to avoid hanging.
So make sure your environment has multicores available.


### 3.3 Run on Docker

If you would like to run the task on Docker container, execute `run_docker` script as following:

```sh
$ ./run_docker --input ./inputs --output ./outputs

# if you want to remove image after finished task
$ ./run_docker -i ./inputs -o ./outputs --rmi
```

(Note: currently running on a single container)

### 3.4 Custom task

The structure is following:
```cpp
// app/custom_mr.cpp
#include "mapreduce.h"

using namespace mapreduce;

class SomeMapper : public Mapper<key_type, value_type>
{
 public:
  void map(const std::string &input, const Context &context)
  {
    /// do something

    /// output mapping via context.write(key_type, value_type)
    context.write(key, value);  // this is for sending data
  }

class SomeReducer : public Reducer<mapped_key_type,
                                   mapped_value_type,
                                   output_key_type,
                                   output_value_type>
{
 public:
  void reduce(const std::string &key, const std::vector<long> &value,
              const Context &context)
  {
    /// do something

    /// output the result via context.write(output_key_type, output_value_type)
    context.write(key, value);  // this is for sending data
  }
}

int main(int &, char *[])
{
  FileFormat fmt{};
  fmt.add_input_path("./inputs");
  fmt.set_output_path("./outputs");

  Job<SomeMapper, SomeReducer> job{};
  job.set_file_format(fmt);

  job.run();  // execute mapreduce task

  return 0;
}
```

Put every scripts in `./app` directory. 
Then, update `app/sourcelist.cmake` like the following:
```
# list all related source/header files
target_sources(main PRIVATE
  ${PROJECT_SOURCE_DIR}/app/custom_mr.cpp
              :
)
```

Note that currently `key_type` only accepts `std::string`
and `value_type` accepts either `int` or `long`.

## <a name="4-performance"></a>4 Performance

To check the performance, tested with simple word counting task described in "Execution example" section with small text dataset.
The dataset is consisted of random texts with 12000 files (2000 original texts with copying), in total 16M words.

The data size is in total 93.4 MB.

For testing, following two machines are used.

  - Ubuntu 20.04, Intel Xeon E5-1620, 16GB RAM (Master, Workers)
  - Ubuntu 20.04, Intel core i5-3550, 8GB RAM (Workers)

For baseline, used the first machine, and for MapReduce version, machines are conencted with ethernet cables within the same LAN.
The script used for baseline is `./baseline.cc`.

Additionally, made sure other processes were idle or ran few processes which would not affect performance checks.

The result was shown in the following table.

| Baseline | SimpleMapReduce</br>(1 machine, 1 master, 3 workers) | SimpleMapReduce</br>(2 machines, 1 master, 7 workers) |
|--|--|--|
|14.065 sec. | 5.275 sec. | 7.700 sec. |

If the datasize is small, the overhead of network connection is large,
so it can see benefit only when processing the large dataset both in terms of number of files and the file sizes.

## <a name="5-todo"></a>5 TODO

- Add configurations (mid)
- Performance test with publicly available dataset (mid) (with details including source link)
- Test with other compilers (e.g. clang++)
- Testings(high)
- Add more types to process (currently only string for keys and int,long for values) (mid)
- Error handling (high)
- Check performance by connecting more machines (high)
- Able to add more mapper layers (mid)

## <a name="6-references"></a>6 References

<a name="ref1"></a>\[1\] Jeffrey Dean, Sanjay Ghemawat; MapReduce: Simplified Data Processing on Large Clusters; 2004

<a name="ref2"></a>\[2\] Open-MPI website [[link](https://www.open-mpi.org/)]

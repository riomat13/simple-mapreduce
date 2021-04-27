# Simple MapReduce

This is a project to build simple *mapreduce*[[1](#ref1)] app with *C++* using *MPI*[[2](#ref2)].

### Disclaimer

Mapreduce built in this project may not be a right structured.
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
|   ├─ movielens/        # example app to compute movie rating mean
|   ├─ rainfall/         # example app for secondary sort using CompositeKey
|   ├─ wordcount/        # example app to count words in texts
|   └─ wordcount_with_combiner/
|                        # example app to count words using Combiner
|
├─ cmake/          # contains files used for build
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

You can use `clang++` instead of `g++`(tested with `clang++-9` and `clang++-10`),
however, this uses `<filesystem>` so that make sure `libstdc++` has it,
such as, on Ubuntu 18.04 or Ubuntu 16.04.
(Easiest way is install `g++>=9`)

Optionally, you can use `libtbb-dev` on Ubuntu 20.04.

If you use alternatives such as `MPICH`, check their website and install it manually.
(tested on Ubuntu Docker container locally with `mpich-3.4.1`)

### 2.2 Network settings
This project is run with 2 PCs to check performance. The result is described at [section4](#4-performance).

If you would like to run as distributed system, use *Network File System(NFS)*.
This requires additional tasks for network settings or configurations, such as SSH settings.
Documentation says MPI uses arbitorary ports so to make it easier, temporarily turn off firewall or open for the machines.
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

# to see options for MapReduce app
$ ./run_task --help

# set input/output directories with -i/--input and -o/--output options
$ mpirun [MPI options] ./run_task --input ./inputs --output ./outputs

# if there are multiple input directories, separate by commas
$ mpirun [MPI options] ./run_task -i ./inputs1,./inputs2,./inputs3 -o ./outputs

# for instance,
# use all node on the local machine
$ mpirun ./run_task
# use machines set in `host_file` and 4 nodes in total
$ mpirun -np 4 --hostfile host_file ./run_task  -i ./inputs -o ./outputs
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

Target app to build can be chosen as following:
```sh
# for word count with combiner
$ ./run_docker --app wordcount-with-combiner -i ./inputs -o ./outputs

# for calculating movie rating mean with movielens dataset
$ ./run_docker --app movielens -i ./inputs -o ./outputs

# for custom app
$ ./run_docker --app custom -i ./inputs -o ./outputs
```

(Note: currently running on a single container)

### 3.4 Another example
In order to run another example(calculating movie rating mean),
run the following commands,

```sh
$ cd app
$ mkdir -p build && cd build
$ cmake -DSIMPLEMR_BUILD_APP_TYPE=movielens ..
$ make -j
```

### 3.5 Custom task

The structure is following:
```cpp
// app/custom_mr.cpp
#include "mapreduce.h"

using namespace mapreduce;

class SomeMapper
    : public Mapper<in_key_type, in_value_type, out_key_type, out_value_type> {
 public:
  void map(const in_key_type &key, const in_value_type &value,
           const Context<out_key_type, out_value_type> &context) {
    out_key_type out_key;
    out_value_type out_value;

    /// do something

    /// output mapping via context.write(out_key_type, out_value_type)
    context.write(out_key, out_value);  // this is for sending data
  }

class SomeReducer
    : public Reducer<in_key_type, in_value_type, out_key_type, out_value_type> {
 public:
  void reduce(const in_key_type &key, const std::vector<in_value_type> &value,
              const Context<out_key_type, out_value_type> &context) {
    out_key_type out_key;
    out_value_type out_value;

    /// do something

    /// for summation or calculating mean
    /// REDUCE_SUM() and REDUCE_MEAN() can be used respectively
    /// See `app/word_count.cc` as an example

    /// output the result via context.write(out_key_type, out_value_type)
    context.write(out_key, out_value);  // this is for sending data
  }
}

int main(int &, char *[]) {
  Job job{};

  /// Set input/output directories, if you would like to set on source file.
  /// However, recommended to pass by command line arguments instead for flexibility
  job.add_input_path("./inputs1");
  job.add_input_path("./inputs2");
  job.set_output_path("./outputs");   // command line argument will override this

  /// Set Mapper and Reducer (pass as template name)
  job.set_mapper<SomeMapper>();
  job.set_reducer<SomeReducer>();

  job.run();  // execute mapreduce task

  return 0;
}
```

Data type can be chosen from `String`, `Int16`, `Int`/`Int32`, `Long`/`Int64`, `Float`, `Double` in `mapreduce::type` as both key and value.
For instance,
```
using namespace mapreduce::type;

class SomeMapper : Mapper<String, Long, Long, Double> {...}
```

Put every scripts in `./app` directory,
and update `app/sourcelist.cmake` like the following:
```
# list all related source/header files
target_sources(main PRIVATE
  ${PROJECT_SOURCE_DIR}/main.cc
              :
)
```

Then, run the following commands on terminal.
```sh
$ cd app
$ mkdir -p build && cd build
$ cmake -DSIMPLEMR_BUILD_APP_TYPE=custom ..
$ make -j
```

## <a name="4-performance"></a>4 Performance

To check the performance, tested with simple word counting task described in "Execution example" section with publicly available dataset.
The dataset used in this performance testing is "Large Movie Review Dataset"[\[3\]](#ref3) ([link](https://ai.stanford.edu/~amaas/data/sentiment/))
which consists of 25,000 files for training, 25,000 files for testing and 50,000 unlabeled files.

The data size is in total 132.0 MB.

### 4.1 Preprocessing

In order to utilize parallel processing, merged several files into a file by taking steps:

  - make groups in each group(train/test) and each category(pos/neg/unsup)
  - group files by the first letter (e.g. `1*.txt` will be group 1)
  - copy the dataset (tested with double and quadruple dataset)

In short, files in "pos" groups in train dataset starting with "5" is going to be merged as the same one file,
therefore, some files has large size, and some are small.

Now, there are 50 original files (pos/neg/unsup in train and pos/neg in test with 10 files each) and copied them.
The total data size is *264.0MB*(double) and *528.0MB*(quadruple).

### 4.2 Environment
For testing, following two machines are used.

  - Ubuntu 20.04, Intel Xeon E5-1620 3.60GHz, 16GB RAM (Master, Workers)
  - Ubuntu 20.04, Intel core i5-3550 3.30GHz, 8GB RAM (Workers)

For baseline, used the first machine, and for MapReduce version, machines are conencted with ethernet cables through same switch.
The script used for baseline is `./baseline.cc` and compiled with `g++-9`.

Additionally, made sure other processes were idle or ran few processes which would not affect performance checks.

### 4.3 Result

The performance is tested with original dataset (100K files) and preprocessed them (200 files, see section 4.1).
The tests are run three times each and took the middle.
Time score can be vary so that this is just an example result.

The first table shows results from original dataset without copy.

| Baseline | SimpleMapReduce</br>(1 PC, 1 master, 3 workers) | SimpleMapReduce</br>(2 PCs, 1 master, 7 workers) |
|--|--|--|
| 23.733 sec. | 12.223 sec. | 10.499 sec. |

and the following one is from preprocessed dataset.

| Baseline | SimpleMapReduce</br>(1 PC, 1 master, 3 workers) | SimpleMapReduce</br>(2 PCs, 1 master, 7 workers) | SimpleMapReduce</br>with Combiner</br>(2 PCs, 1 master, 7 workers) |
|--|--|--|--|
| 41.412 sec. | 22.530 sec. | 13.925 sec. | 8.271 sec. |

With *264.0MB* dataset, processing with combiner could acheive ***5.01x*** faster than sequential processing.
(The code is located at `app/wordcount_with_combiner/main.cc`)

When reading 100K files, it involves many network communications which are overhead for this process,
so that it does not have big improvement to use multiple machines.

Whereas with preprocessed files, it has less communications so that it could utilize the parallel processing.

Additionally, tested with more doubled dataset (in total *528.0MB*).

| Baseline | SimpleMapReduce</br>(2 PCs, 1 master, 7 workers) | SimpleMapReduce</br>with Combiner</br>(2 PCs, 1 master, 7 workers)
|--|--|--|
| 82.990 sec. | 30.747 sec. | 15.457 sec. |

With *Combiner*, the processing becomes ***5.37x*** faster than baseline.
Thus, the larger the original dataset, the more it can utilize.

### 4.4 Another example
As an another example, calculate movie rating mean using *MovieLens 20M dataset*[[4](#ref4)].

The details is described in [link](./app/movielens/README.md).

## <a name="5-todo"></a>5 TODO

- Run on distributed memory (low)
- Refactoring
  - Network communication
  - Configuration

## <a name="6-references"></a>6 References

<a name="ref1"></a>\[1\] Jeffrey Dean, Sanjay Ghemawat; MapReduce: Simplified Data Processing on Large Clusters; 2004

<a name="ref2"></a>\[2\] Open-MPI website [[link](https://www.open-mpi.org/)]
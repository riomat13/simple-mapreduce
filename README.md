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
5. [Further Improvements](#5-improvements)
6. [References](#6-references)

## <a name="1-project-structure"></a>1 Project structure

The directory is structured as following.

```
.
├─ cmake/         # contains file(s) used for build
├─ example/       # contains example word count app
|                   (this will be a main app as a submission)
├─ include/       # directory containing documents and related items
├─ inputs/        # directory to store input files to process
├─ outputs/       # directory to store processed output
|                   (this will be generated if not exist)
|                   (inputs/ and outputs/ can be vary depending on the scripts)
├─ src/           # contains all source files used for mapreduce
|
├─ baseline.cc    # baseline script for performance comparison
├─ CMakeLists.txt
├─ Makefile       # simplified step (used for development)
└─ README.md
```

## <a name="2-setup"></a>2 Setup
### 2.1 Install dependencies

This needs to install `cmake`, `g++ (>=8)` and `mpi`.
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

## <a name="3-execute-example"></a>3 Execute example

In this section, an example to run a mapreduce task to count words in texts will be shown.

### Word count

As an example task, the popular preprocessing steps "word count" is adopted,
because it is simple and easy to implement, but good for checking performances.

The tokenizing processes are:

  - Remove punctuations
  - Split by space or tabs

The word counting app itself is located at `./example/word_count.cc`.

Note that, in the app script, relative paths are used
so that it needs to follow the commands below otherwise crash when execute the code.

```sh
$ mkdir -p build
$ cd build
$ cmake ..
$ make -j
$ cd ..
$ mpirun [options] build/main
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

### 3.2 Custom task

The structure is following:
```cpp
// main.cpp
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

Then, update `example.sourcelist.cmake` to the following:
```
target_sources(main PRIVATE
  ${SimpleMapReduce_SOURCE_DIR}/example/{a name of the created file}.cc
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

| Baseline | SimpleMapReduce(1 master, 7 workers) |
|--|--|
|14.065 sec. | 7.700 sec. |

If the datasize is small, the overhead of network connection is large so it can see the benefit only when processing the large dataset both in terms of number of files and the file sizes.
(Baseline is still faster using 4000 files with 30+ MB)

## <a name="5-improvements"></a>5 Further Improvements

- Add configurations(mid)
- Restructure the project(mid)
- Performance test with publicly available dataset (mid) (with details including source link)
- Testings(high)
- Packaging(mid)
- Add more types to process (currently only string for keys and int,long for values) (mid)
- Error handling (high)
- Check performance by connecting more machines (high)
- Able to add more mapper layers (mid)

## <a name="6-references"></a>6 References

<a name="ref1"></a>\[1\] Jeffrey Dean, Sanjay Ghemawat; MapReduce: Simplified Data Processing on Large Clusters; 2004

<a name="ref2"></a>\[2\] Open-MPI website [[link](https://www.open-mpi.org/)]
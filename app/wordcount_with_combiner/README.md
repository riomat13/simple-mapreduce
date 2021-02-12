# Word Count App with Combiner

This is an directory containing the script for word count which is identical to `../wordcount`
except that this is using *Combiner*.

The *Combiner* is a reduce process running before shuffle,
therefore it can reduce for shuffling and sorting processes on some tasks.

The word counting is one of the tasks where *Combiner* can be applied.

In order to build this, go to project home directory and then
```sh
$ mkdir -p build && cd build
$ cmake -DSIMPLEMR_BUILD_APP_TYPE=wordcount-with-combiner ..
$ make -j
$ cd ..
$ mpirun [options] ./run_task
```
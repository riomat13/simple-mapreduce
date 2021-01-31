#ifndef SIMPLEMAPREDUCE_H_
#define SIMPLEMAPREDUCE_H_

/// Main mapreduce class to define tasks
#include "simplemapreduce/mapper.h"
#include "simplemapreduce/reducer.h"

/// Processed data handler
#include "simplemapreduce/ops/context.h"

/// Mapreduce task runner
#include "simplemapreduce/ops/job.h"

/// Set directory
#include "simplemapreduce/ops/fileformat.h"

#endif
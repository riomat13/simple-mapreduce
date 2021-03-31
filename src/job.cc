#include "simplemapreduce/ops/job.h"

#include <chrono>
#include <fstream>
#include <future>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <type_traits>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/local/fileformat.h"
#include "simplemapreduce/local/manager.h"
#include "simplemapreduce/local/runner.h"
#include "simplemapreduce/proc/writer.h"
#include "simplemapreduce/util/argparse.h"
#include "simplemapreduce/util/parser.h"

namespace fs = std::filesystem;

using namespace mapreduce::base;
using namespace mapreduce::commons;
using namespace mapreduce::data;
using namespace mapreduce::local;
using namespace mapreduce::proc;
using namespace mapreduce::util;

namespace mapreduce {

/* --------------------------------------------------
 *   Constructor/Destructor
 * -------------------------------------------------- */
Job::Job() {
  /// TODO: set local/distributed
  file_fmt_ = std::make_unique<LocalFileFormat>();

  /// Start networking
  MPI_Init(nullptr, nullptr);
  start_up();
}

Job::Job(int &argc, char *argv[]) {
  ArgParser parser{argc, argv};

  /// Not to run if -h/--help option is passed
  if (parser.is_help()) {
    is_valid_ = false;
    return;
  }

  /// TODO: set local/distributed
  file_fmt_ = std::make_unique<LocalFileFormat>();

  /// Start networking
  MPI_Init(&argc, &argv);
  start_up();

  auto inputs = parse_string(parser.get_option("input"));
  if (!inputs.empty())
    file_fmt_->add_input_paths(inputs);
  
  auto output = parser.get_option("output");
  file_fmt_->set_output_path(std::move(output));
}

void Job::start_up() {
  int mpi_rank, mpi_size;
  MPI_Comm_rank(MPI_COMM_WORLD, &(mpi_rank));
  MPI_Comm_size(MPI_COMM_WORLD, &(mpi_size));

  if (mpi_size == 1) {
    logger.error("No worker node is provieded. Aborting...");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  conf_->worker_size = mpi_size - 1;

  if (mpi_rank == 0) {
    /// Set up master node
    is_master_ = true;

    /// TODO: set local/distributed
    job_manager_ = std::make_unique<LocalJobManager>();
    job_manager_->set_conf(conf_);
  } else {
    /// Set up worker nodes
    /// TODO: set local/distributed
    job_runner_ = std::make_unique<LocalJobRunner>();
    job_runner_->set_conf(conf_);

    /// Exclude master node as a count of workers
    /// This will be used for later computation and identifiers
    conf_->worker_rank = mpi_rank - 1;
  }
}

Job::~Job() {
  /// Close connection
  if (is_valid_)
    MPI_Finalize();
}

/* --------------------------------------------------
 *   Setup
 * -------------------------------------------------- */
void Job::add_input_path(const std::string& path) {
  file_fmt_->add_input_path(std::move(path));
}

void Job::set_output_path(const std::string& path) {
  file_fmt_->set_output_path(std::move(path));
}

void Job::setup_tmp_dir() {
  /// Clear all directory if exists
  if (fs::exists(conf_->tmpdir)) {
    for (const auto& p: fs::directory_iterator(conf_->tmpdir)) {
      fs::remove_all(p);
    }
  } else {
    /// create a new one if not exist
    fs::create_directory(conf_->tmpdir);
  }
}

void Job::setup_output_dir() {
  /// Clear all directory if exists
  if (fs::exists(file_fmt_->get_output_path())) {
    for (const auto& p: fs::directory_iterator(file_fmt_->get_output_path())) {
      fs::remove_all(p);
    }
  } else {
    /// create a new one if not exist
    fs::create_directories(file_fmt_->get_output_path());
  }
}

/* --------------------------------------------------
 *   Start Job
 * -------------------------------------------------- */
int Job::run() {
  /// Not run when -h/--help option is passed
  if (!is_valid_) return 0;

  /// send target file to each node
  if (is_master_) {
    if (!has_mapper_ || !has_reducer_)
      throw std::runtime_error("Mapper and/or Reducer is not set.");

    /// Set up a directory to store intermediate state data
    setup_tmp_dir();
    setup_output_dir();

    job_manager_->set_file_format(std::move(file_fmt_));
    job_manager_->start();

    fs::remove_all(conf_->tmpdir);
    logger.info("[Master] Finished task");
  } else {
    conf_->output_dirpath = file_fmt_->get_output_path();
    job_runner_->start();
    logger.debug("[Worker] Finished all tasks on worker ", conf_->worker_rank);
  }

  return 0;
}
  
}  // namespace mapreduce
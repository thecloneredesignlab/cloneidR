.cellseg_output_subdirs <- function() {
  c("DetectionResults", "Annotations", "Images", "Confluency", "Masks")
}

.cellseg_ingest_pattern <- function(id) {
  paste0("^", id, "_([0-9]+x_ph|t1|t2|flair|pd)")
}

.cellseg_read_config <- function() {
  yml <- yaml::read_yaml(paste0(system.file(package = "cloneid"), "/config/config.yaml"))
  cfg <- yml$cellSegmentation
  if (is.null(cfg)) {
    stop("Missing cellSegmentation config section")
  }

  backend <- cfg$backend
  if (is.null(backend) || !nzchar(trimws(as.character(backend)))) {
    backend <- "local"
  }

  list(
    backend = tolower(trimws(as.character(backend))),
    input = cfg$input,
    output = cfg$output,
    tmp = cfg$tmp,
    bucket = cfg$bucket,
    region = cfg$region,
    endpoint = cfg$endpoint,
    inputPrefix = cfg$inputPrefix,
    outputPrefix = cfg$outputPrefix
  )
}

.cellseg_is_s3 <- function(config = .cellseg_read_config()) {
  identical(config$backend, "s3")
}

.cellseg_is_local <- function(config = .cellseg_read_config()) {
  identical(config$backend, "local")
}

.cellseg_durable_paths <- function(config = .cellseg_read_config()) {
  if (.cellseg_is_s3(config)) {
    stop("S3 cellSegmentation backend is configured but package durable-storage migration is not complete yet.")
  }
  list(
    input = paste0(normalizePath(config$input), "/"),
    output = paste0(normalizePath(config$output), "/")
  )
}

.cellseg_tmp_dir <- function(config = .cellseg_read_config()) {
  normalizePath(config$tmp)
}

.cellseg_paths <- function() {
  cfg <- .cellseg_read_config()
  c(.cellseg_durable_paths(cfg), list(tmp = .cellseg_tmp_dir(cfg)))
}

.cellseg_delete_paths <- function(id, input_root, output_root) {
  del_in <- list.files(
    input_root,
    pattern = .cellseg_ingest_pattern(id),
    full.names = TRUE
  )
  del_in <- grep("\\.tif$", del_in, value = TRUE, ignore.case = TRUE)
  if (length(del_in) > 0) file.remove(del_in)

  for (sub in .cellseg_output_subdirs()) {
    del_out <- list.files(
      file.path(output_root, sub),
      pattern = .cellseg_ingest_pattern(id),
      full.names = TRUE
    )
    if (length(del_out) > 0) file.remove(del_out)
  }

  invisible(NULL)
}

.cellseg_delete_id_artifacts <- function(id, config = .cellseg_read_config()) {
  if (.cellseg_is_s3(config)) {
    stop("S3 cellSegmentation backend cleanup is not implemented yet.")
  }

  paths <- .cellseg_paths()
  .cellseg_delete_paths(id, paths$input, paths$output)
}

.cellseg_copy_files <- function(files, destination_dir) {
  if (length(files) == 0) {
    return(invisible(logical(0)))
  }

  dir.create(destination_dir, recursive = TRUE, showWarnings = FALSE)
  ok <- file.copy(files, destination_dir)
  if (!all(ok)) {
    stop(paste0("Failed to copy all files into ", destination_dir))
  }
  invisible(ok)
}

.cellseg_ensure_output_dirs <- function(config = .cellseg_read_config()) {
  if (.cellseg_is_s3(config)) {
    stop("S3 cellSegmentation output directory initialization is not implemented yet.")
  }

  output_root <- .cellseg_paths()$output
  for (subdir in .cellseg_output_subdirs()) {
    suppressWarnings(dir.create(paste0(output_root, subdir)))
  }

  invisible(output_root)
}

.cellseg_list_input_files <- function(id, config = .cellseg_read_config()) {
  if (.cellseg_is_s3(config)) {
    stop("S3 cellSegmentation input listing is not implemented yet.")
  }

  input_root <- .cellseg_paths()$input
  files <- list.files(input_root, pattern = paste0("^", id, "_"), full.names = TRUE)
  files <- grep("x_ph_", files, value = TRUE)
  files <- grep("\\.tif$", files, value = TRUE, ignore.case = TRUE)
  files
}

.cellseg_clear_output_files <- function(id, config = .cellseg_read_config()) {
  if (.cellseg_is_s3(config)) {
    stop("S3 cellSegmentation output cleanup is not implemented yet.")
  }

  output_root <- .cellseg_paths()$output
  for (subfolder in .cellseg_output_subdirs()) {
    files <- list.files(
      paste0(output_root, subfolder),
      pattern = .cellseg_ingest_pattern(id),
      full.names = TRUE
    )
    file.remove(files)
  }

  invisible(NULL)
}

.cellseg_stage_inputs_to_tmp <- function(id, tmp_dir, config = .cellseg_read_config()) {
  if (.cellseg_is_s3(config)) {
    stop("S3 cellSegmentation temporary staging is not implemented yet.")
  }

  files <- .cellseg_list_input_files(id, config = config)
  dir.create(tmp_dir, recursive = TRUE, showWarnings = FALSE)
  if (length(files) > 0) {
    ok <- file.copy(files, tmp_dir)
    if (!all(ok)) {
      stop(paste0("Failed to stage all input files into temporary directory for id ", id))
    }
  }
  files
}

.cellseg_list_output_files <- function(id, subdir, config = .cellseg_read_config()) {
  if (.cellseg_is_s3(config)) {
    stop("S3 cellSegmentation output listing is not implemented yet.")
  }

  output_root <- .cellseg_paths()$output
  list.files(
    paste0(output_root, subdir),
    pattern = .cellseg_ingest_pattern(id),
    full.names = TRUE
  )
}

.cellseg_wait_for_analysis_output <- function(id, how_many, timeout = 120, config = .cellseg_read_config()) {
  if (.cellseg_is_s3(config)) {
    stop("S3 cellSegmentation output wait loop is not implemented yet.")
  }

  output_root <- .cellseg_paths()$output
  print(paste0("Waiting for ", id, " to appear under ", output_root, " ..."), quote = FALSE)

  f_o <- character(0)
  start_time <- Sys.time()
  while (length(f_o) < how_many && as.numeric(difftime(Sys.time(), start_time, units = "secs")) < timeout) {
    Sys.sleep(3)
    f_o <- .cellseg_list_output_files(id, "Images", config = config)
  }

  if (length(f_o) < how_many) {
    warning("Timed out waiting for analysis output.")
    return()
  }

  f <- .cellseg_list_output_files(id, "DetectionResults", config = config)
  f_a <- .cellseg_list_output_files(id, "Annotations", config = config)
  f_c <- .cellseg_list_output_files(id, "Confluency", config = config)
  f_c <- grep("\\.csv$", f_c, value = TRUE, ignore.case = TRUE)

  print(
    paste0("Output found for ", fileparts(f_o[1])$name, " and ", (length(f_o) - 1), " other image files."),
    quote = FALSE
  )
  list(f = f, f_a = f_a, f_o = f_o, f_c = f_c)
}

.cellseg_get_mri_files <- function(id, signal = "t2", config = .cellseg_read_config()) {
  if (.cellseg_is_s3(config)) {
    stop("S3 cellSegmentation MRI file lookup is not implemented yet.")
  }

  output_root <- .cellseg_paths()$output
  input_root <- .cellseg_paths()$input
  signal <- gsub("_cavity", "", signal)

  list(
    mask = list.files(
      paste0(output_root, "/Images"),
      pattern = paste0("^", id, "_", signal),
      full.names = TRUE
    )[1],
    raw = list.files(
      input_root,
      pattern = paste0("^", id, "_", signal),
      full.names = TRUE
    )[1]
  )
}

.cellseg_copy_to_input <- function(files, config = .cellseg_read_config()) {
  if (.cellseg_is_s3(config)) {
    stop("S3 cellSegmentation input promotion is not implemented yet.")
  }

  .cellseg_copy_files(files, .cellseg_paths()$input)
}

.cellseg_copy_to_output <- function(files, subdir, config = .cellseg_read_config()) {
  if (.cellseg_is_s3(config)) {
    stop("S3 cellSegmentation output promotion is not implemented yet.")
  }

  .cellseg_copy_files(files, paste0(.cellseg_paths()$output, subdir))
}

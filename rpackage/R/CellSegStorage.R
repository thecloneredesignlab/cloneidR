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

.cellseg_paths <- function() {
  cfg <- .cellseg_read_config()
  if (.cellseg_is_s3(cfg)) {
    stop("S3 cellSegmentation backend is configured but package durable-storage migration is not complete yet.")
  }
  list(
    input = paste0(normalizePath(cfg$input), "/"),
    output = paste0(normalizePath(cfg$output), "/"),
    tmp = normalizePath(cfg$tmp)
  )
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

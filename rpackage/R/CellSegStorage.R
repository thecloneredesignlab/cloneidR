.cellseg_output_subdirs <- function() {
  c("DetectionResults", "Annotations", "Images", "Confluency", "Masks")
}

.cellseg_s3_normalize_component <- function(x) {
  x <- if (is.null(x)) "" else as.character(x)
  x <- trimws(x)
  x <- gsub("^/+", "", x)
  x <- gsub("/+$", "", x)
  x
}

.cellseg_s3_join <- function(...) {
  parts <- list(...)
  parts <- unlist(parts, use.names = FALSE)
  parts <- vapply(parts, .cellseg_s3_normalize_component, character(1))
  parts <- parts[nzchar(parts)]
  paste(parts, collapse = "/")
}

.cellseg_s3_bucket_name <- function(config = .cellseg_read_config()) {
  bucket <- config$bucket
  if (is.null(bucket) || !nzchar(trimws(as.character(bucket)))) {
    stop("Missing cellSegmentation.bucket for s3 backend")
  }
  bucket <- sub("^s3://", "", trimws(as.character(bucket)))
  bucket <- sub("/.*$", "", bucket)
  bucket
}

.cellseg_s3_input_prefix <- function(config = .cellseg_read_config()) {
  prefix <- config$inputPrefix
  if (is.null(prefix) || !nzchar(trimws(as.character(prefix)))) {
    prefix <- "inputs"
  }
  .cellseg_s3_normalize_component(prefix)
}

.cellseg_s3_output_prefix <- function(subdir = NULL, config = .cellseg_read_config()) {
  prefix <- config$outputPrefix
  if (is.null(prefix) || !nzchar(trimws(as.character(prefix)))) {
    prefix <- "outputs"
  }
  .cellseg_s3_join(prefix, subdir)
}

.cellseg_s3_uri <- function(prefix, config = .cellseg_read_config()) {
  prefix <- .cellseg_s3_normalize_component(prefix)
  suffix <- if (nzchar(prefix)) paste0("/", prefix, "/") else "/"
  paste0("s3://", .cellseg_s3_bucket_name(config), suffix)
}

.cellseg_is_s3_uri <- function(path) {
  is.character(path) && length(path) == 1 && grepl("^s3://", path)
}

.cellseg_parse_s3_uri <- function(uri) {
  if (!.cellseg_is_s3_uri(uri)) {
    stop("Expected s3:// URI")
  }
  stripped <- sub("^s3://", "", uri)
  bucket <- sub("/.*$", "", stripped)
  remainder <- sub("^[^/]+/?", "", stripped)
  list(
    bucket = bucket,
    prefix = .cellseg_s3_normalize_component(remainder)
  )
}

.cellseg_s3_args <- function(config = .cellseg_read_config()) {
  args <- list(
    bucket = .cellseg_s3_bucket_name(config)
  )
  if (!is.null(config$region) && nzchar(trimws(as.character(config$region)))) {
    args$region <- trimws(as.character(config$region))
  }
  if (!is.null(config$endpoint) && nzchar(trimws(as.character(config$endpoint)))) {
    args$base_url <- trimws(as.character(config$endpoint))
    args$url_style <- "path"
  }
  args
}

.cellseg_require_s3_package <- function() {
  if (!requireNamespace("aws.s3", quietly = TRUE)) {
    stop("S3 cellSegmentation backend requires the aws.s3 package to be installed.")
  }
  invisible(TRUE)
}

.cellseg_s3_extract_keys <- function(objects) {
  if (length(objects) == 0) {
    return(character(0))
  }
  keys <- vapply(
    objects,
    function(obj) {
      if (is.null(obj)) {
        return(NA_character_)
      }
      if (!is.null(obj[["Key"]])) {
        return(as.character(obj[["Key"]]))
      }
      if (!is.null(attr(obj, "Key"))) {
        return(as.character(attr(obj, "Key")))
      }
      NA_character_
    },
    character(1)
  )
  keys[!is.na(keys)]
}

.cellseg_s3_list_keys <- function(prefix, config = .cellseg_read_config()) {
  .cellseg_require_s3_package()
  args <- c(
    list(prefix = .cellseg_s3_normalize_component(prefix), max = Inf),
    .cellseg_s3_args(config)
  )
  objects <- do.call(aws.s3::get_bucket, args)
  .cellseg_s3_extract_keys(objects)
}

.cellseg_s3_upload_file <- function(file, key, config = .cellseg_read_config()) {
  .cellseg_require_s3_package()
  args <- c(
    list(file = file, object = .cellseg_s3_normalize_component(key)),
    .cellseg_s3_args(config)
  )
  do.call(aws.s3::put_object, args)
}

.cellseg_s3_download_file <- function(key, file, config = .cellseg_read_config()) {
  .cellseg_require_s3_package()
  dir.create(dirname(file), recursive = TRUE, showWarnings = FALSE)
  args <- c(
    list(object = .cellseg_s3_normalize_component(key), file = file, overwrite = TRUE),
    .cellseg_s3_args(config)
  )
  do.call(aws.s3::save_object, args)
}

.cellseg_s3_delete_key <- function(key, config = .cellseg_read_config()) {
  .cellseg_require_s3_package()
  args <- c(
    list(object = .cellseg_s3_normalize_component(key)),
    .cellseg_s3_args(config)
  )
  do.call(aws.s3::delete_object, args)
}

.cellseg_s3_cache_dir <- function(category, config = .cellseg_read_config(), id = NULL, subdir = NULL) {
  file.path(.cellseg_tmp_dir(config), ".cellseg-s3-cache", category, subdir %||% "", id %||% "")
}

.cellseg_s3_materialize_keys <- function(keys, destination_dir, config = .cellseg_read_config()) {
  if (length(keys) == 0) {
    return(character(0))
  }

  dir.create(destination_dir, recursive = TRUE, showWarnings = FALSE)
  local_files <- file.path(destination_dir, basename(keys))
  for (i in seq_along(keys)) {
    .cellseg_s3_download_file(keys[i], local_files[i], config = config)
  }
  local_files
}

.cellseg_match_keys <- function(keys, pattern) {
  if (is.null(keys) || length(keys) == 0) {
    return(character(0))
  }

  keys <- unname(as.character(keys))
  keys[grepl(pattern, basename(keys))]
}

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0) y else x
}

.cellseg_input_artifact_pattern <- function(id) {
  paste0(.cellseg_ingest_pattern(id), ".*\\.tif$")
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
    return(list(
      input = .cellseg_s3_uri(.cellseg_s3_input_prefix(config), config),
      output = .cellseg_s3_uri(.cellseg_s3_output_prefix(config = config), config)
    ))
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
  if (.cellseg_is_s3_uri(input_root) || .cellseg_is_s3_uri(output_root)) {
    if (!(.cellseg_is_s3_uri(input_root) && .cellseg_is_s3_uri(output_root))) {
      stop("Input and output roots must both be local or both be s3:// URIs")
    }

    input_info <- .cellseg_parse_s3_uri(input_root)
    output_info <- .cellseg_parse_s3_uri(output_root)
    config <- .cellseg_read_config()
    config$bucket <- input_info$bucket
    input_keys <- .cellseg_match_keys(
      .cellseg_s3_list_keys(input_info$prefix, config = config),
      .cellseg_input_artifact_pattern(id)
    )
    if (length(input_keys) > 0) {
      invisible(vapply(input_keys, .cellseg_s3_delete_key, logical(1), config = config))
    }

    for (sub in .cellseg_output_subdirs()) {
      prefix <- .cellseg_s3_join(output_info$prefix, sub)
      out_keys <- .cellseg_match_keys(
        .cellseg_s3_list_keys(prefix, config = config),
        .cellseg_ingest_pattern(id)
      )
      if (length(out_keys) > 0) {
        invisible(vapply(out_keys, .cellseg_s3_delete_key, logical(1), config = config))
      }
    }

    return(invisible(NULL))
  }

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

.cellseg_delete_input_artifacts <- function(id, input_root = NULL, config = .cellseg_read_config()) {
  if (is.null(input_root)) {
    input_root <- .cellseg_durable_paths(config)$input
  }

  if (.cellseg_is_s3_uri(input_root)) {
    input_info <- .cellseg_parse_s3_uri(input_root)
    config$bucket <- input_info$bucket
    input_keys <- .cellseg_match_keys(
      .cellseg_s3_list_keys(input_info$prefix, config = config),
      .cellseg_input_artifact_pattern(id)
    )
    if (length(input_keys) > 0) {
      invisible(vapply(input_keys, .cellseg_s3_delete_key, logical(1), config = config))
    }
    return(invisible(input_keys))
  }

  input_files <- list.files(
    input_root,
    pattern = .cellseg_ingest_pattern(id),
    full.names = TRUE
  )
  input_files <- grep("\\.tif$", input_files, value = TRUE, ignore.case = TRUE)
  if (length(input_files) > 0) {
    file.remove(input_files)
  }
  invisible(input_files)
}

.cellseg_delete_id_artifacts <- function(id, config = .cellseg_read_config()) {
  paths <- c(.cellseg_durable_paths(config), list(tmp = .cellseg_tmp_dir(config)))
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
    return(invisible(.cellseg_durable_paths(config)$output))
  }

  output_root <- .cellseg_durable_paths(config)$output
  for (subdir in .cellseg_output_subdirs()) {
    suppressWarnings(dir.create(paste0(output_root, subdir)))
  }

  invisible(output_root)
}

.cellseg_list_input_files <- function(id, config = .cellseg_read_config()) {
  if (.cellseg_is_s3(config)) {
    keys <- .cellseg_match_keys(
      .cellseg_s3_list_keys(.cellseg_s3_input_prefix(config), config = config),
      paste0("^", id, "_.*x_ph_.*\\.tif$")
    )
    cache_dir <- .cellseg_s3_cache_dir("input", config = config, id = id)
    return(.cellseg_s3_materialize_keys(keys, cache_dir, config = config))
  }

  input_root <- .cellseg_durable_paths(config)$input
  files <- list.files(input_root, pattern = paste0("^", id, "_"), full.names = TRUE)
  files <- grep("x_ph_", files, value = TRUE)
  files <- grep("\\.tif$", files, value = TRUE, ignore.case = TRUE)
  files
}

.cellseg_clear_output_files <- function(id, config = .cellseg_read_config()) {
  if (.cellseg_is_s3(config)) {
    for (subfolder in .cellseg_output_subdirs()) {
      keys <- .cellseg_match_keys(
        .cellseg_s3_list_keys(.cellseg_s3_output_prefix(subfolder, config), config = config),
        .cellseg_ingest_pattern(id)
      )
      if (length(keys) > 0) {
        invisible(vapply(keys, .cellseg_s3_delete_key, logical(1), config = config))
      }
    }
    return(invisible(NULL))
  }

  output_root <- .cellseg_durable_paths(config)$output
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
    keys <- .cellseg_match_keys(
      .cellseg_s3_list_keys(.cellseg_s3_output_prefix(subdir, config), config = config),
      .cellseg_ingest_pattern(id)
    )
    cache_dir <- .cellseg_s3_cache_dir("output", config = config, id = id, subdir = subdir)
    return(.cellseg_s3_materialize_keys(keys, cache_dir, config = config))
  }

  output_root <- .cellseg_durable_paths(config)$output
  list.files(
    paste0(output_root, subdir),
    pattern = .cellseg_ingest_pattern(id),
    full.names = TRUE
  )
}

.cellseg_list_input_artifacts <- function(id, input_root = NULL, config = .cellseg_read_config()) {
  if (is.null(input_root)) {
    input_root <- .cellseg_durable_paths(config)$input
  }

  if (.cellseg_is_s3_uri(input_root)) {
    input_info <- .cellseg_parse_s3_uri(input_root)
    config$bucket <- input_info$bucket
    keys <- .cellseg_match_keys(
      .cellseg_s3_list_keys(input_info$prefix, config = config),
      .cellseg_input_artifact_pattern(id)
    )
    if (length(keys) == 0) {
      return(character(0))
    }
    return(paste0("s3://", config$bucket, "/", keys))
  }

  files <- list.files(
    input_root,
    pattern = .cellseg_ingest_pattern(id),
    full.names = TRUE
  )
  grep("\\.tif$", files, value = TRUE, ignore.case = TRUE)
}

.cellseg_list_output_artifacts <- function(id, subdir, output_root = NULL, config = .cellseg_read_config()) {
  if (is.null(output_root)) {
    output_root <- .cellseg_durable_paths(config)$output
  }

  if (.cellseg_is_s3_uri(output_root)) {
    output_info <- .cellseg_parse_s3_uri(output_root)
    config$bucket <- output_info$bucket
    prefix <- .cellseg_s3_join(output_info$prefix, subdir)
    keys <- .cellseg_match_keys(
      .cellseg_s3_list_keys(prefix, config = config),
      .cellseg_ingest_pattern(id)
    )
    if (length(keys) == 0) {
      return(character(0))
    }
    return(paste0("s3://", config$bucket, "/", keys))
  }

  list.files(
    file.path(output_root, subdir),
    pattern = .cellseg_ingest_pattern(id),
    full.names = TRUE
  )
}

.cellseg_list_mri_transient_files <- function(id, transient_dir) {
  mri_regex <- paste0("^", id, "_(t1|t2|flair|pd)")
  list(
    mask = list.files(transient_dir, pattern = paste0(mri_regex, ".*_msk\\.nii"), full.names = TRUE),
    cavity = list.files(transient_dir, pattern = paste0(mri_regex, ".*_cavity\\.nii"), full.names = TRUE),
    raw = {
      raw <- list.files(transient_dir, pattern = paste0(mri_regex, "\\.nii"), full.names = TRUE)
      raw[!grepl("_msk\\.nii|_cavity\\.nii", raw)]
    }
  )
}

.cellseg_validate_mri_transient_dir <- function(id, transient_dir) {
  ingest_regex <- .cellseg_ingest_pattern(id)
  matched <- list.files(transient_dir, pattern = ingest_regex, full.names = TRUE)
  all_files <- list.files(transient_dir, full.names = TRUE)
  if (length(matched) != length(all_files)) {
    stop(paste0("All files in (", transient_dir, ") must match ingest regex for id: ", id))
  }

  files <- .cellseg_list_mri_transient_files(id, transient_dir)
  if (length(files$mask) != 1) {
    stop(paste0("Expected exactly 1 mask NIfTI for id ", id, "; found ", length(files$mask)))
  }
  if (length(files$raw) != 1) {
    stop(paste0("Expected exactly 1 raw NIfTI for id ", id, "; found ", length(files$raw)))
  }

  files
}

.cellseg_promote_mri_files <- function(id, transient_dir, config = .cellseg_read_config()) {
  files <- .cellseg_validate_mri_transient_dir(id, transient_dir)
  .cellseg_copy_to_input(files$raw[1], config = config)
  .cellseg_copy_to_output(files$mask[1], "Images", config = config)
  if (length(files$cavity) == 1) {
    .cellseg_copy_to_output(files$cavity[1], "Images", config = config)
  }

  invisible(files)
}

.cellseg_list_promoted_mri_masks <- function(id, config = .cellseg_read_config()) {
  files <- .cellseg_list_output_files(id, "Images", config = config)
  files[grepl(
    pattern = paste0("^", id, "_(t1|t2|flair|pd).*_msk\\.nii$"),
    x = basename(files)
  )]
}

.cellseg_wait_for_analysis_output <- function(id, how_many, timeout = 120, config = .cellseg_read_config()) {
  if (.cellseg_is_s3(config)) {
    output_root <- .cellseg_durable_paths(config)$output
    print(paste0("Waiting for ", id, " to appear under ", output_root, " ..."), quote = FALSE)

    image_keys <- character(0)
    start_time <- Sys.time()
    image_prefix <- .cellseg_s3_output_prefix("Images", config)
    while (length(image_keys) < how_many && as.numeric(difftime(Sys.time(), start_time, units = "secs")) < timeout) {
      Sys.sleep(3)
      image_keys <- .cellseg_match_keys(
        .cellseg_s3_list_keys(image_prefix, config = config),
        .cellseg_ingest_pattern(id)
      )
    }

    if (length(image_keys) < how_many) {
      warning("Timed out waiting for analysis output.")
      return()
    }

    f_o <- .cellseg_list_output_files(id, "Images", config = config)
    f <- .cellseg_list_output_files(id, "DetectionResults", config = config)
    f_a <- .cellseg_list_output_files(id, "Annotations", config = config)
    f_c <- .cellseg_list_output_files(id, "Confluency", config = config)
    f_c <- grep("\\.csv$", f_c, value = TRUE, ignore.case = TRUE)

    print(
      paste0("Output found for ", fileparts(f_o[1])$name, " and ", (length(f_o) - 1), " other image files."),
      quote = FALSE
    )
    return(list(f = f, f_a = f_a, f_o = f_o, f_c = f_c))
  }

  output_root <- .cellseg_durable_paths(config)$output
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
    signal <- gsub("_cavity", "", signal)
    mask_files <- .cellseg_list_output_files(id, "Images", config = config)
    mask_files <- mask_files[grep(
      pattern = paste0("^", id, "_", signal),
      x = basename(mask_files),
      value = FALSE
    )]
    input_keys <- .cellseg_match_keys(
      .cellseg_s3_list_keys(.cellseg_s3_input_prefix(config), config = config),
      paste0("^", id, "_", signal, ".*\\.nii$")
    )
    raw_cache_dir <- .cellseg_s3_cache_dir("input", config = config, id = id)
    raw_files <- .cellseg_s3_materialize_keys(input_keys, raw_cache_dir, config = config)
    return(list(mask = mask_files[1], raw = raw_files[1]))
  }

  output_root <- .cellseg_durable_paths(config)$output
  input_root <- .cellseg_durable_paths(config)$input
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
    keys <- .cellseg_s3_join(.cellseg_s3_input_prefix(config), basename(files))
    ok <- vapply(
      seq_along(files),
      function(i) .cellseg_s3_upload_file(files[i], keys[i], config = config),
      logical(1)
    )
    if (!all(ok)) {
      stop("Failed to upload all files into s3 cellSegmentation input prefix")
    }
    return(invisible(ok))
  }

  .cellseg_copy_files(files, .cellseg_durable_paths(config)$input)
}

.cellseg_copy_to_output <- function(files, subdir, config = .cellseg_read_config()) {
  if (.cellseg_is_s3(config)) {
    keys <- .cellseg_s3_join(.cellseg_s3_output_prefix(subdir, config), basename(files))
    ok <- vapply(
      seq_along(files),
      function(i) .cellseg_s3_upload_file(files[i], keys[i], config = config),
      logical(1)
    )
    if (!all(ok)) {
      stop(paste0("Failed to upload all files into s3 cellSegmentation output prefix for ", subdir))
    }
    return(invisible(ok))
  }

  .cellseg_copy_files(files, paste0(.cellseg_durable_paths(config)$output, subdir))
}

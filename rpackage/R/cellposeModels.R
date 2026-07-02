.cloneid_model_cache_dir <- function() {
  cache_dir <- Sys.getenv("CLONEID_MODEL_DIR", unset = NA)
  if (is.na(cache_dir) || !nzchar(cache_dir)) {
    cache_dir <- file.path(path.expand("~"), ".cache", "cloneid", "models")
  }
  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  normalizePath(cache_dir, mustWork = TRUE)
}

.read_cellpose_model_manifest <- function() {
  manifest_file <- system.file(
    "python",
    "cellpose_model_manifest.tsv",
    package = "cloneid"
  )
  if (!nzchar(manifest_file) || !file.exists(manifest_file)) {
    return(data.frame())
  }
  utils::read.delim(
    manifest_file,
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
}

.manifest_value <- function(manifest_row, field) {
  if (!field %in% names(manifest_row)) {
    return("")
  }
  value <- as.character(manifest_row[[field]][1])
  if (is.na(value)) {
    return("")
  }
  value
}

.verify_cellpose_model <- function(path, manifest_row) {
  if (!file.exists(path)) {
    return(FALSE)
  }
  expected_size <- .manifest_value(manifest_row, "size_bytes")
  if (nzchar(expected_size)) {
    expected_size <- as.numeric(expected_size)
    if (is.na(expected_size) || file.info(path)$size != expected_size) {
      return(FALSE)
    }
  }
  expected_md5 <- .manifest_value(manifest_row, "md5")
  if (nzchar(expected_md5)) {
    return(identical(unname(tools::md5sum(path)), expected_md5))
  }
  TRUE
}

.download_cellpose_model <- function(model_name, manifest_row) {
  cache_dir <- .cloneid_model_cache_dir()
  model_path <- file.path(cache_dir, model_name)

  if (.verify_cellpose_model(model_path, manifest_row)) {
    return(normalizePath(model_path, mustWork = TRUE))
  }

  url <- .manifest_value(manifest_row, "url")
  if (!nzchar(url)) {
    stop("No download URL is configured for Cellpose model: ", model_name)
  }

  tmp_path <- paste0(model_path, ".download")
  if (file.exists(tmp_path)) {
    unlink(tmp_path)
  }

  message("Downloading Cellpose model from Zenodo: ", model_name)
  utils::download.file(
    url,
    tmp_path,
    mode = "wb",
    quiet = FALSE
  )

  if (!.verify_cellpose_model(tmp_path, manifest_row)) {
    unlink(tmp_path)
    stop("Downloaded Cellpose model failed checksum/size verification: ", model_name)
  }

  if (file.exists(model_path)) {
    unlink(model_path)
  }
  if (!file.rename(tmp_path, model_path)) {
    stop("Failed to move downloaded Cellpose model into cache: ", model_path)
  }
  normalizePath(model_path, mustWork = TRUE)
}

.resolve_cellpose_model <- function(model_name) {
  if (is.null(model_name) || is.na(model_name) || !nzchar(model_name)) {
    stop("Missing Cellpose model name")
  }

  if (file.exists(model_name)) {
    return(normalizePath(model_name, mustWork = TRUE))
  }

  packaged_model <- file.path(find.package("cloneid"), "python", model_name)
  if (file.exists(packaged_model)) {
    return(normalizePath(packaged_model, mustWork = TRUE))
  }

  manifest <- .read_cellpose_model_manifest()
  if (!nrow(manifest) || !"name" %in% names(manifest)) {
    stop("Cellpose model is not bundled and no model manifest is available: ", model_name)
  }

  match <- manifest[manifest$name == model_name, , drop = FALSE]
  if (!nrow(match)) {
    stop("Cellpose model is not bundled and is not listed in the model manifest: ", model_name)
  }

  .download_cellpose_model(model_name, match[1, , drop = FALSE])
}

fetchCellposeModel <- function(model_name = "cellposeSAM_train_2025_09_30_20250930_165546") {
  .resolve_cellpose_model(model_name)
}

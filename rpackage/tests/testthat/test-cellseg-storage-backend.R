library(testthat)

.cfg     <- cloneid:::.cellseg_read_config
.paths   <- cloneid:::.cellseg_paths
.durable <- cloneid:::.cellseg_durable_paths
.tmpdirf <- cloneid:::.cellseg_tmp_dir
.is_s3   <- cloneid:::.cellseg_is_s3
.is_local<- cloneid:::.cellseg_is_local
.deletep <- cloneid:::.cellseg_delete_paths
.delete_input <- cloneid:::.cellseg_delete_input_artifacts
.delete_artifacts <- cloneid:::.cellseg_delete_id_artifacts
.copy_in <- cloneid:::.cellseg_copy_to_input
.copy_out<- cloneid:::.cellseg_copy_to_output
.subs    <- cloneid:::.cellseg_output_subdirs
.stage_in<- cloneid:::.cellseg_stage_inputs_to_tmp
.clear_out <- cloneid:::.cellseg_clear_output_files
.list_out <- cloneid:::.cellseg_list_output_files
.list_input_artifacts <- cloneid:::.cellseg_list_input_artifacts
.list_output_artifacts <- cloneid:::.cellseg_list_output_artifacts
.list_mri_transient <- cloneid:::.cellseg_list_mri_transient_files
.validate_mri_transient <- cloneid:::.cellseg_validate_mri_transient_dir
.promote_mri <- cloneid:::.cellseg_promote_mri_files
.list_mri_masks <- cloneid:::.cellseg_list_promoted_mri_masks
.s3_input_prefix <- cloneid:::.cellseg_s3_input_prefix
.s3_output_prefix <- cloneid:::.cellseg_s3_output_prefix
.s3_uri <- cloneid:::.cellseg_s3_uri

test_that("setupCLONEID stamps local cell segmentation paths into installed config", {
  pkg_dir <- tempfile("cloneid-pkg-")
  dir.create(file.path(pkg_dir, "config"), recursive = TRUE)
  cfg_path <- file.path(pkg_dir, "config", "config.yaml")
  yaml::write_yaml(list(
    mysqlConnection = list(
      host = "localhost",
      port = 3306,
      user = "",
      password = "",
      database = "CLONEID",
      schemaScript = "CLONEID_schema.sql"
    ),
    cellSegmentation = list(
      backend = "local",
      input = "~/CellSegmentations/",
      output = "~/CellSegmentations/output/",
      tmp = "~/Downloads/tmp/",
      bucket = NULL,
      region = NULL,
      endpoint = NULL,
      inputPrefix = "inputs",
      outputPrefix = "outputs"
    )
  ), cfg_path)

  indir <- file.path(tempdir(), "cellseg-local-input")
  outdir <- file.path(tempdir(), "cellseg-local-output")
  tmpdir <- file.path(tempdir(), "cellseg-local-tmp")
  dir.create(indir, recursive = TRUE, showWarnings = FALSE)
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
  dir.create(tmpdir, recursive = TRUE, showWarnings = FALSE)

  with_mocked_bindings(
    system.file = function(..., package = NULL) {
      if (!identical(package, "cloneid")) stop("unexpected package")
      pkg_dir
    },
    .package = "base",
    code = {
      cloneid::setupCLONEID(
        cellseg_input = indir,
        cellseg_output = outdir,
        cellseg_tmp = tmpdir,
        cellseg_backend = "local"
      )

      stamped <- .cfg()
      expect_identical(stamped$backend, "local")
      expect_identical(stamped$input, indir)
      expect_identical(stamped$output, outdir)
      expect_identical(stamped$tmp, tmpdir)

      paths <- .paths()
      expect_identical(paths$input, paste0(normalizePath(indir), "/"))
      expect_identical(paths$output, paste0(normalizePath(outdir), "/"))
      expect_identical(paths$tmp, normalizePath(tmpdir))
    }
  )
})

test_that("setupCLONEID stamps s3 cell segmentation config while tmp remains local", {
  pkg_dir <- tempfile("cloneid-pkg-")
  dir.create(file.path(pkg_dir, "config"), recursive = TRUE)
  cfg_path <- file.path(pkg_dir, "config", "config.yaml")
  yaml::write_yaml(list(
    mysqlConnection = list(
      host = "localhost",
      port = 3306,
      user = "",
      password = "",
      database = "CLONEID",
      schemaScript = "CLONEID_schema.sql"
    ),
    cellSegmentation = list(
      backend = "local",
      input = "~/CellSegmentations/",
      output = "~/CellSegmentations/output/",
      tmp = "~/Downloads/tmp/",
      bucket = NULL,
      region = NULL,
      endpoint = NULL,
      inputPrefix = "inputs",
      outputPrefix = "outputs"
    )
  ), cfg_path)

  tmpdir <- file.path(tempdir(), "cellseg-s3-tmp")
  dir.create(tmpdir, recursive = TRUE, showWarnings = FALSE)

  with_mocked_bindings(
    system.file = function(..., package = NULL) {
      if (!identical(package, "cloneid")) stop("unexpected package")
      pkg_dir
    },
    .package = "base",
    code = {
      cloneid::setupCLONEID(
        cellseg_input = "s3://cloneid4mysql8/CellSegmentations/input",
        cellseg_output = "s3://cloneid4mysql8/CellSegmentations/output",
        cellseg_tmp = tmpdir,
        cellseg_backend = "s3",
        cellseg_bucket = "cloneid4mysql8",
        cellseg_region = "us-east-1",
        cellseg_input_prefix = "CellSegmentations/input",
        cellseg_output_prefix = "CellSegmentations/output"
      )

      stamped <- .cfg()
      expect_identical(stamped$backend, "s3")
      expect_identical(stamped$bucket, "cloneid4mysql8")
      expect_identical(stamped$region, "us-east-1")
      expect_identical(stamped$inputPrefix, "CellSegmentations/input")
      expect_identical(stamped$outputPrefix, "CellSegmentations/output")
      expect_identical(stamped$input, "s3://cloneid4mysql8/CellSegmentations/input")
      expect_identical(stamped$output, "s3://cloneid4mysql8/CellSegmentations/output")

      paths <- .paths()
      expect_identical(paths$input, "s3://cloneid4mysql8/CellSegmentations/input/")
      expect_identical(paths$output, "s3://cloneid4mysql8/CellSegmentations/output/")
      expect_identical(paths$tmp, normalizePath(tmpdir))
    }
  )
})

test_that(".cellseg_read_config defaults backend to local when absent", {
  with_mocked_bindings(
    read_yaml = function(...) list(cellSegmentation = list(
      input = "/tmp/input",
      output = "/tmp/output",
      tmp = "/tmp/tmp"
    )),
    .package = "yaml",
    code = {
      result <- .cfg()

      expect_identical(result$backend, "local")
      expect_identical(result$input, "/tmp/input")
      expect_identical(result$output, "/tmp/output")
      expect_identical(result$tmp, "/tmp/tmp")
    }
  )
})

test_that(".cellseg_is_local and .cellseg_is_s3 reflect backend selection", {
  expect_true(.is_local(list(backend = "local")))
  expect_false(.is_s3(list(backend = "local")))
  expect_true(.is_s3(list(backend = "s3")))
  expect_false(.is_local(list(backend = "s3")))
})

test_that(".cellseg_paths returns normalized local paths and exposes s3 URIs when configured", {
  local_cfg <- list(
    backend = "local",
    input = tempdir(),
    output = tempdir(),
    tmp = tempdir()
  )

  local_paths <- with_mocked_bindings(
    .cellseg_read_config = function() local_cfg,
    .package = "cloneid",
    code = .paths()
  )

  expect_match(local_paths$input, "/$")
  expect_match(local_paths$output, "/$")
  expect_true(nzchar(local_paths$tmp))

  s3_paths <- with_mocked_bindings(
    .cellseg_read_config = function() list(
      backend = "s3",
      bucket = "cloneid4mysql8",
      inputPrefix = "inputs",
      outputPrefix = "outputs",
      tmp = tempdir()
    ),
    .package = "cloneid",
    code = .paths()
  )

  expect_identical(s3_paths$input, "s3://cloneid4mysql8/inputs/")
  expect_identical(s3_paths$output, "s3://cloneid4mysql8/outputs/")
  expect_true(nzchar(s3_paths$tmp))
})

test_that(".cellseg_durable_paths emits s3 URIs while .cellseg_tmp_dir remains local-only", {
  s3_cfg <- list(
    backend = "s3",
    input = "/unused/input",
    output = "/unused/output",
    tmp = tempdir(),
    bucket = "s3://cloneid4mysql8",
    inputPrefix = "inputs",
    outputPrefix = "outputs"
  )

  durable <- .durable(s3_cfg)
  expect_identical(durable$input, "s3://cloneid4mysql8/inputs/")
  expect_identical(durable$output, "s3://cloneid4mysql8/outputs/")

  expect_identical(
    .tmpdirf(s3_cfg),
    normalizePath(s3_cfg$tmp)
  )
})

test_that("s3 artifact listings return character(0) when no keys match", {
  s3_cfg <- list(
    backend = "s3",
    input = "/unused/input",
    output = "/unused/output",
    tmp = tempdir(),
    bucket = "cloneid4mysql8",
    inputPrefix = "CellSegmentations/input",
    outputPrefix = "CellSegmentations/output"
  )

  with_mocked_bindings(
    .cellseg_s3_list_keys = function(...) NULL,
    .package = "cloneid",
    code = {
      expect_identical(
        .list_input_artifacts("CASE123", config = s3_cfg),
        character(0)
      )
      expect_identical(
        .list_output_artifacts("CASE123", "Images", config = s3_cfg),
        character(0)
      )
    }
  )
})

test_that(".cellseg_delete_paths removes only matching id-scoped artifacts", {
  root <- tempfile("cellseg-")
  dir.create(root)
  indir <- file.path(root, "input")
  outdir <- file.path(root, "output")
  dir.create(indir)
  for (sub in .subs()) dir.create(file.path(outdir, sub), recursive = TRUE)

  keep_in <- file.path(indir, "OTHER_10x_ph_bl.tif")
  rm_in <- file.path(indir, "CASE123_10x_ph_bl.tif")
  file.create(keep_in)
  file.create(rm_in)

  keep_out <- file.path(outdir, "Images", "OTHER_10x_ph_bl_overlay.png")
  rm_out <- file.path(outdir, "Images", "CASE123_10x_ph_bl_overlay.png")
  file.create(keep_out)
  file.create(rm_out)

  .deletep("CASE123", indir, outdir)

  expect_true(file.exists(keep_in))
  expect_false(file.exists(rm_in))
  expect_true(file.exists(keep_out))
  expect_false(file.exists(rm_out))
})

test_that(".cellseg_delete_input_artifacts removes only matching input artifacts", {
  root <- tempfile("cellseg-delete-input-")
  dir.create(root)
  indir <- file.path(root, "input")
  dir.create(indir)

  keep_in <- file.path(indir, "OTHER_10x_ph_bl.tif")
  rm_in <- file.path(indir, "CASE123_10x_ph_bl.tif")
  file.create(keep_in)
  file.create(rm_in)

  .delete_input("CASE123", indir)

  expect_true(file.exists(keep_in))
  expect_false(file.exists(rm_in))
})

test_that(".cellseg_delete_input_artifacts ignores prefix-collision and invalid filenames", {
  root <- tempfile("cellseg-delete-input-collision-")
  dir.create(root)
  indir <- file.path(root, "input")
  dir.create(indir)

  target <- file.path(indir, "CASE123_10x_ph_bl.tif")
  keep_prefix_collision <- file.path(indir, "CASE1234_10x_ph_bl.tif")
  keep_missing_separator <- file.path(indir, "CASE12310x_ph_bl.tif")
  keep_wrong_modality <- file.path(indir, "CASE123_xx_ph_bl.tif")
  keep_wrong_extension <- file.path(indir, "CASE123_10x_ph_bl.txt")
  keep_other <- file.path(indir, "OTHER_10x_ph_bl.tif")

  vapply(
    c(target, keep_prefix_collision, keep_missing_separator, keep_wrong_modality, keep_wrong_extension, keep_other),
    function(path) { writeLines("x", path); TRUE },
    logical(1)
  )

  .delete_input("CASE123", indir)

  expect_false(file.exists(target))
  expect_true(file.exists(keep_prefix_collision))
  expect_true(file.exists(keep_missing_separator))
  expect_true(file.exists(keep_wrong_modality))
  expect_true(file.exists(keep_wrong_extension))
  expect_true(file.exists(keep_other))
})

test_that(".cellseg_delete_id_artifacts removes durable artifacts through package config and leaves tmp untouched", {
  root <- tempfile("cellseg-delete-artifacts-")
  dir.create(root)
  indir <- file.path(root, "input")
  outdir <- file.path(root, "output")
  tmpdir <- file.path(root, "tmp")
  dir.create(indir)
  dir.create(outdir)
  dir.create(tmpdir)
  for (sub in .subs()) dir.create(file.path(outdir, sub), recursive = TRUE)

  durable_in <- file.path(indir, "CASE123_10x_ph_bl.tif")
  durable_out <- file.path(outdir, "Images", "CASE123_10x_ph_bl_overlay.png")
  transient_tmp <- file.path(tmpdir, "CASE123_transient.tmp")
  writeLines("x", durable_in)
  writeLines("y", durable_out)
  writeLines("z", transient_tmp)

  mock_cfg <- list(
    backend = "local",
    input = indir,
    output = outdir,
    tmp = tmpdir
  )

  with_mocked_bindings(
    .cellseg_read_config = function() mock_cfg,
    .package = "cloneid",
    code = .delete_artifacts("CASE123")
  )

  expect_false(file.exists(durable_in))
  expect_false(file.exists(durable_out))
  expect_true(file.exists(transient_tmp))
})

test_that(".cellseg_delete_paths removes only exact id-scoped durable artifacts across subdirs", {
  root <- tempfile("cellseg-delete-paths-exact-")
  dir.create(root)
  indir <- file.path(root, "input")
  outdir <- file.path(root, "output")
  dir.create(indir)
  dir.create(outdir)
  for (sub in .subs()) dir.create(file.path(outdir, sub), recursive = TRUE)

  target_in <- file.path(indir, "CASE123_10x_ph_bl.tif")
  keep_in_prefix_collision <- file.path(indir, "CASE1234_10x_ph_bl.tif")
  keep_in_wrong_signal <- file.path(indir, "CASE123_xx_ph_bl.tif")
  writeLines("x", target_in)
  writeLines("x", keep_in_prefix_collision)
  writeLines("x", keep_in_wrong_signal)

  target_out <- file.path(outdir, "Images", "CASE123_10x_ph_bl_overlay.png")
  keep_out_prefix_collision <- file.path(outdir, "Images", "CASE1234_10x_ph_bl_overlay.png")
  keep_out_missing_separator <- file.path(outdir, "Images", "CASE12310x_ph_bl_overlay.png")
  keep_other_subdir <- file.path(outdir, "Masks", "OTHER_10x_ph_bl_cp_masks.png")
  writeLines("y", target_out)
  writeLines("y", keep_out_prefix_collision)
  writeLines("y", keep_out_missing_separator)
  writeLines("y", keep_other_subdir)

  .deletep("CASE123", indir, outdir)

  expect_false(file.exists(target_in))
  expect_false(file.exists(target_out))
  expect_true(file.exists(keep_in_prefix_collision))
  expect_true(file.exists(keep_in_wrong_signal))
  expect_true(file.exists(keep_out_prefix_collision))
  expect_true(file.exists(keep_out_missing_separator))
  expect_true(file.exists(keep_other_subdir))
})

test_that(".remove_id_artifacts respects explicit caller-supplied durable roots", {
  root <- tempfile("cellseg-explicit-cleanup-")
  dir.create(root)
  indir <- file.path(root, "input")
  outdir <- file.path(root, "output")
  tmpdir <- file.path(root, "tmp")
  dir.create(indir)
  dir.create(outdir)
  dir.create(tmpdir)
  for (sub in .subs()) dir.create(file.path(outdir, sub), recursive = TRUE)

  durable_in <- file.path(indir, "CASE123_10x_ph_bl.tif")
  durable_out <- file.path(outdir, "Images", "CASE123_10x_ph_bl_overlay.png")
  transient_tmp <- file.path(tmpdir, "CASE123_transient.tmp")
  writeLines("x", durable_in)
  writeLines("y", durable_out)
  writeLines("z", transient_tmp)

  cloneid:::.remove_id_artifacts("CASE123", indir, outdir)

  expect_false(file.exists(durable_in))
  expect_false(file.exists(durable_out))
  expect_true(file.exists(transient_tmp))
})

test_that(".cellseg_copy_to_input and .cellseg_copy_to_output copy files in local mode", {
  root <- tempfile("cellseg-copy-")
  dir.create(root)
  indir <- file.path(root, "input")
  outdir <- file.path(root, "output")
  tmpdir <- file.path(root, "tmp")
  dir.create(indir)
  dir.create(outdir)
  dir.create(tmpdir)
  for (sub in .subs()) dir.create(file.path(outdir, sub), recursive = TRUE)

  src_file <- file.path(root, "CASE123_10x_ph_bl.tif")
  writeLines("x", src_file)

  mock_cfg <- list(
    backend = "local",
    input = indir,
    output = outdir,
    tmp = tmpdir
  )

  with_mocked_bindings(
    .cellseg_read_config = function() mock_cfg,
    .package = "cloneid",
    code = {
      .copy_in(src_file)
      .copy_out(src_file, "Images")
    }
  )

  expect_true(file.exists(file.path(indir, basename(src_file))))
  expect_true(file.exists(file.path(outdir, "Images", basename(src_file))))
})

test_that("s3 durable promotion uploads files without moving transient storage into s3", {
  root <- tempfile("cellseg-s3-copy-")
  dir.create(root)
  tmpdir <- file.path(root, "tmp")
  dir.create(tmpdir)
  src_file <- file.path(root, "CASE123_10x_ph_bl.tif")
  writeLines("x", src_file)

  mock_cfg <- list(
    backend = "s3",
    tmp = tmpdir,
    bucket = "cloneid4mysql8",
    inputPrefix = "inputs",
    outputPrefix = "outputs"
  )

  uploads <- list()
  with_mocked_bindings(
    .cellseg_read_config = function() mock_cfg,
    .cellseg_s3_upload_file = function(file, key, config) {
      uploads[[length(uploads) + 1]] <<- list(file = file, key = key, tmp = config$tmp)
      TRUE
    },
    .package = "cloneid",
    code = {
      .copy_in(src_file)
      .copy_out(src_file, "Images")
    }
  )

  expect_identical(length(uploads), 2L)
  expect_identical(uploads[[1]]$key, "inputs/CASE123_10x_ph_bl.tif")
  expect_identical(uploads[[2]]$key, "outputs/Images/CASE123_10x_ph_bl.tif")
  expect_identical(uploads[[1]]$tmp, tmpdir)
})

test_that(".cellseg_stage_inputs_to_tmp stages microscopy inputs and .cellseg_clear_output_files removes matching outputs", {
  root <- tempfile("cellseg-stage-")
  dir.create(root)
  indir <- file.path(root, "input")
  outdir <- file.path(root, "output")
  tmpdir <- file.path(root, "tmp")
  dir.create(indir)
  dir.create(outdir)
  dir.create(tmpdir)
  for (sub in .subs()) dir.create(file.path(outdir, sub), recursive = TRUE)

  src_match <- file.path(indir, "CASE123_10x_ph_bl.tif")
  src_other <- file.path(indir, "OTHER_10x_ph_bl.tif")
  writeLines("x", src_match)
  writeLines("y", src_other)

  out_match <- file.path(outdir, "Images", "CASE123_10x_ph_bl_overlay.png")
  out_other <- file.path(outdir, "Images", "OTHER_10x_ph_bl_overlay.png")
  writeLines("x", out_match)
  writeLines("y", out_other)

  mock_cfg <- list(
    backend = "local",
    input = indir,
    output = outdir,
    tmp = tmpdir
  )

  staged <- with_mocked_bindings(
    .cellseg_read_config = function() mock_cfg,
    .package = "cloneid",
    code = .stage_in("CASE123", file.path(tmpdir, "CASE123"))
  )

  expect_identical(basename(staged), "CASE123_10x_ph_bl.tif")
  expect_true(file.exists(file.path(tmpdir, "CASE123", "CASE123_10x_ph_bl.tif")))

  with_mocked_bindings(
    .cellseg_read_config = function() mock_cfg,
    .package = "cloneid",
    code = .clear_out("CASE123")
  )

  expect_false(file.exists(out_match))
  expect_true(file.exists(out_other))
})

test_that(".cellseg_list_output_files returns id-scoped files from the requested subdir", {
  root <- tempfile("cellseg-list-")
  dir.create(root)
  indir <- file.path(root, "input")
  outdir <- file.path(root, "output")
  tmpdir <- file.path(root, "tmp")
  dir.create(indir)
  dir.create(outdir)
  dir.create(tmpdir)
  for (sub in .subs()) dir.create(file.path(outdir, sub), recursive = TRUE)

  match_file <- file.path(outdir, "DetectionResults", "CASE123_10x_ph_bl_pred.csv")
  other_file <- file.path(outdir, "DetectionResults", "OTHER_10x_ph_bl_pred.csv")
  writeLines("x", match_file)
  writeLines("y", other_file)

  mock_cfg <- list(
    backend = "local",
    input = indir,
    output = outdir,
    tmp = tmpdir
  )

  files <- with_mocked_bindings(
    .cellseg_read_config = function() mock_cfg,
    .package = "cloneid",
    code = .list_out("CASE123", "DetectionResults")
  )

  expect_identical(files, match_file)
})

test_that(".cellseg_validate_mri_transient_dir enforces local transient MRI input shape", {
  root <- tempfile("cellseg-mri-transient-")
  dir.create(root)

  raw <- file.path(root, "CASE123_t2.nii")
  mask <- file.path(root, "CASE123_t2_msk.nii")
  cavity <- file.path(root, "CASE123_t2_cavity.nii")
  writeLines("raw", raw)
  writeLines("mask", mask)
  writeLines("cavity", cavity)

  files <- .validate_mri_transient("CASE123", root)

  expect_identical(files$raw, raw)
  expect_identical(files$mask, mask)
  expect_identical(files$cavity, cavity)

  bad <- file.path(root, "UNRELATED.txt")
  writeLines("bad", bad)
  expect_error(
    .validate_mri_transient("CASE123", root),
    "must match ingest regex"
  )
})

test_that(".cellseg_promote_mri_files copies only durable MRI assets from transient local source", {
  root <- tempfile("cellseg-mri-promote-")
  dir.create(root)
  indir <- file.path(root, "input")
  outdir <- file.path(root, "output")
  tmpdir <- file.path(root, "tmp")
  transient_dir <- file.path(root, "transient")
  dir.create(indir)
  dir.create(outdir)
  dir.create(tmpdir)
  dir.create(transient_dir)
  for (sub in .subs()) dir.create(file.path(outdir, sub), recursive = TRUE)

  raw <- file.path(transient_dir, "CASE123_t2.nii")
  mask <- file.path(transient_dir, "CASE123_t2_msk.nii")
  cavity <- file.path(transient_dir, "CASE123_t2_cavity.nii")
  writeLines("raw", raw)
  writeLines("mask", mask)
  writeLines("cavity", cavity)

  mock_cfg <- list(
    backend = "local",
    input = indir,
    output = outdir,
    tmp = tmpdir
  )

  promoted <- with_mocked_bindings(
    .cellseg_read_config = function() mock_cfg,
    .package = "cloneid",
    code = .promote_mri("CASE123", transient_dir)
  )

  expect_identical(promoted$raw, raw)
  expect_identical(promoted$mask, mask)
  expect_identical(promoted$cavity, cavity)
  expect_true(file.exists(file.path(indir, basename(raw))))
  expect_true(file.exists(file.path(outdir, "Images", basename(mask))))
  expect_true(file.exists(file.path(outdir, "Images", basename(cavity))))
  expect_true(file.exists(raw))
  expect_true(file.exists(mask))
  expect_true(file.exists(cavity))

  promoted_masks <- with_mocked_bindings(
    .cellseg_read_config = function() mock_cfg,
    .package = "cloneid",
    code = .list_mri_masks("CASE123")
  )

  expect_identical(promoted_masks, file.path(outdir, "Images", basename(mask)))
})

test_that(".cellseg_list_promoted_mri_masks supports compressed MRI masks", {
  root <- tempfile("cellseg-mri-promote-gz-")
  dir.create(root)
  indir <- file.path(root, "input")
  outdir <- file.path(root, "output")
  tmpdir <- file.path(root, "tmp")
  transient_dir <- file.path(root, "transient")
  dir.create(indir)
  dir.create(outdir)
  dir.create(tmpdir)
  dir.create(transient_dir)
  for (sub in .subs()) dir.create(file.path(outdir, sub), recursive = TRUE)

  raw <- file.path(transient_dir, "CASE123_t2.nii.gz")
  mask <- file.path(transient_dir, "CASE123_t2_msk.nii.gz")
  cavity <- file.path(transient_dir, "CASE123_t2_cavity.nii.gz")
  writeLines("raw", raw)
  writeLines("mask", mask)
  writeLines("cavity", cavity)

  mock_cfg <- list(
    backend = "local",
    input = indir,
    output = outdir,
    tmp = tmpdir
  )

  with_mocked_bindings(
    .cellseg_read_config = function() mock_cfg,
    .package = "cloneid",
    code = .promote_mri("CASE123", transient_dir)
  )

  promoted_masks <- with_mocked_bindings(
    .cellseg_read_config = function() mock_cfg,
    .package = "cloneid",
    code = .list_mri_masks("CASE123")
  )

  expect_identical(promoted_masks, file.path(outdir, "Images", basename(mask)))
})

test_that("s3 MRI promotion uploads only durable assets from the local transient source", {
  root <- tempfile("cellseg-mri-promote-s3-")
  dir.create(root)
  tmpdir <- file.path(root, "tmp")
  transient_dir <- file.path(root, "transient")
  dir.create(tmpdir)
  dir.create(transient_dir)

  raw <- file.path(transient_dir, "CASE123_t2.nii")
  mask <- file.path(transient_dir, "CASE123_t2_msk.nii")
  cavity <- file.path(transient_dir, "CASE123_t2_cavity.nii")
  writeLines("raw", raw)
  writeLines("mask", mask)
  writeLines("cavity", cavity)

  mock_cfg <- list(
    backend = "s3",
    tmp = tmpdir,
    bucket = "cloneid4mysql8",
    inputPrefix = "inputs",
    outputPrefix = "outputs"
  )

  uploads <- character(0)
  promoted <- with_mocked_bindings(
    .cellseg_read_config = function() mock_cfg,
    .cellseg_s3_upload_file = function(file, key, config) {
      uploads <<- c(uploads, key)
      TRUE
    },
    .package = "cloneid",
    code = .promote_mri("CASE123", transient_dir)
  )

  expect_identical(promoted$raw, raw)
  expect_identical(promoted$mask, mask)
  expect_identical(promoted$cavity, cavity)
  expect_true(any(uploads == "inputs/CASE123_t2.nii"))
  expect_true(any(uploads == "outputs/Images/CASE123_t2_msk.nii"))
  expect_true(any(uploads == "outputs/Images/CASE123_t2_cavity.nii"))
  expect_true(file.exists(raw))
  expect_true(file.exists(mask))
  expect_true(file.exists(cavity))
})

test_that("s3 listing, deletion, and MRI lookup materialize permanent objects locally", {
  root <- tempfile("cellseg-s3-ops-")
  dir.create(root)
  tmpdir <- file.path(root, "tmp")
  dir.create(tmpdir)

  mock_cfg <- list(
    backend = "s3",
    tmp = tmpdir,
    bucket = "cloneid4mysql8",
    inputPrefix = "inputs",
    outputPrefix = "outputs"
  )

  fake_keys <- c(
    "inputs/CASE123_t2.nii",
    "inputs/CASE123_10x_ph_bl.tif",
    "outputs/Images/CASE123_t2_msk.nii",
    "outputs/Images/CASE123_10x_ph_bl_overlay.png",
    "outputs/DetectionResults/CASE123_10x_ph_bl_pred.csv",
    "outputs/Annotations/CASE123_10x_ph_bl_cellpose_count.csv",
    "outputs/Confluency/CASE123_10x_ph_bl_Confluency.csv"
  )
  downloaded <- character(0)
  deleted <- character(0)

  list_keys <- function(prefix, config) {
    fake_keys[startsWith(fake_keys, prefix)]
  }

  download_key <- function(key, file, config) {
    dir.create(dirname(file), recursive = TRUE, showWarnings = FALSE)
    writeLines(key, file)
    downloaded <<- c(downloaded, key)
    file
  }

  delete_key <- function(key, config) {
    deleted <<- c(deleted, key)
    TRUE
  }

  with_mocked_bindings(
    .cellseg_read_config = function() mock_cfg,
    .cellseg_s3_list_keys = list_keys,
    .cellseg_s3_download_file = download_key,
    .cellseg_s3_delete_key = delete_key,
    .package = "cloneid",
    code = {
      input_files <- .stage_in("CASE123", file.path(tmpdir, "CASE123"))
      expect_identical(basename(input_files), "CASE123_10x_ph_bl.tif")
      expect_true(file.exists(file.path(tmpdir, "CASE123", "CASE123_10x_ph_bl.tif")))

      out_files <- .list_out("CASE123", "DetectionResults")
      expect_identical(basename(out_files), "CASE123_10x_ph_bl_pred.csv")
      expect_true(file.exists(out_files))

      mri_masks <- .list_mri_masks("CASE123")
      expect_identical(basename(mri_masks), "CASE123_t2_msk.nii")

      mri_files <- cloneid:::.cellseg_get_mri_files("CASE123", "t2")
      expect_identical(basename(mri_files$raw), "CASE123_t2.nii")
      expect_identical(basename(mri_files$mask), "CASE123_t2_msk.nii")

      .clear_out("CASE123")
      .delete_artifacts("CASE123")
    }
  )

  expect_true(any(grepl("^inputs/CASE123_10x_ph_bl\\.tif$", downloaded)))
  expect_true(any(grepl("^outputs/DetectionResults/CASE123_10x_ph_bl_pred\\.csv$", downloaded)))
  expect_true(any(grepl("^outputs/Images/CASE123_t2_msk\\.nii$", downloaded)))
  expect_true(any(grepl("^outputs/Images/CASE123_10x_ph_bl_overlay\\.png$", deleted)))
  expect_true(any(grepl("^inputs/CASE123_10x_ph_bl\\.tif$", deleted)))
})

test_that("explicit s3 durable roots are respected during cleanup", {
  mock_cfg <- list(
    backend = "s3",
    tmp = tempdir(),
    bucket = "WRONG_BUCKET",
    inputPrefix = "inputs",
    outputPrefix = "outputs"
  )

  listed_prefixes <- character(0)
  deleted <- character(0)

  with_mocked_bindings(
    .cellseg_read_config = function() mock_cfg,
    .cellseg_s3_list_keys = function(prefix, config) {
      listed_prefixes <<- c(listed_prefixes, paste(config$bucket, prefix, sep = "::"))
      if (prefix == "portal-inputs") {
        return("portal-inputs/CASE123_10x_ph_bl.tif")
      }
      if (prefix == "portal-outputs/Images") {
        return("portal-outputs/Images/CASE123_10x_ph_bl_overlay.png")
      }
      character(0)
    },
    .cellseg_s3_delete_key = function(key, config) {
      deleted <<- c(deleted, paste(config$bucket, key, sep = "::"))
      TRUE
    },
    .package = "cloneid",
    code = .deletep("CASE123", "s3://RIGHT_BUCKET/portal-inputs/", "s3://RIGHT_BUCKET/portal-outputs/")
  )

  expect_true(any(listed_prefixes == "RIGHT_BUCKET::portal-inputs"))
  expect_true(any(listed_prefixes == "RIGHT_BUCKET::portal-outputs/Images"))
  expect_true(any(deleted == "RIGHT_BUCKET::portal-inputs/CASE123_10x_ph_bl.tif"))
  expect_true(any(deleted == "RIGHT_BUCKET::portal-outputs/Images/CASE123_10x_ph_bl_overlay.png"))
})

test_that("s3 durable artifact listing returns canonical s3 uris", {
  mock_cfg <- list(
    backend = "s3",
    tmp = tempdir(),
    bucket = "cloneid4mysql8",
    inputPrefix = "inputs",
    outputPrefix = "outputs"
  )

  with_mocked_bindings(
    .cellseg_read_config = function() mock_cfg,
    .cellseg_s3_list_keys = function(prefix, config) {
      switch(prefix,
        "inputs" = c("inputs/CASE123_10x_ph_bl.tif", "inputs/OTHER_10x_ph_bl.tif"),
        "outputs/Images" = c("outputs/Images/CASE123_10x_ph_bl_overlay.png", "outputs/Images/OTHER_10x_ph_bl_overlay.png"),
        character(0)
      )
    },
    .package = "cloneid",
    code = {
      input_uris <- .list_input_artifacts("CASE123")
      output_uris <- .list_output_artifacts("CASE123", "Images")
      expect_identical(input_uris, "s3://cloneid4mysql8/inputs/CASE123_10x_ph_bl.tif")
      expect_identical(output_uris, "s3://cloneid4mysql8/outputs/Images/CASE123_10x_ph_bl_overlay.png")
    }
  )
})

test_that("s3 durable cleanup deletes only exact id-scoped keys under explicit roots", {
  deleted <- character(0)

  with_mocked_bindings(
    .cellseg_read_config = function() list(
      backend = "s3",
      tmp = tempdir(),
      bucket = "WRONG_BUCKET",
      inputPrefix = "wrong-inputs",
      outputPrefix = "wrong-outputs"
    ),
    .cellseg_s3_list_keys = function(prefix, config) {
      switch(prefix,
        "portal-inputs" = c(
          "portal-inputs/CASE123_10x_ph_bl.tif",
          "portal-inputs/CASE1234_10x_ph_bl.tif",
          "portal-inputs/CASE12310x_ph_bl.tif",
          "portal-inputs/CASE123_xx_ph_bl.tif",
          "portal-inputs/OTHER_10x_ph_bl.tif"
        ),
        "portal-outputs/Images" = c(
          "portal-outputs/Images/CASE123_10x_ph_bl_overlay.png",
          "portal-outputs/Images/CASE1234_10x_ph_bl_overlay.png",
          "portal-outputs/Images/CASE12310x_ph_bl_overlay.png",
          "portal-outputs/Images/OTHER_10x_ph_bl_overlay.png"
        ),
        "portal-outputs/DetectionResults" = c(
          "portal-outputs/DetectionResults/CASE123_10x_ph_bl.csv",
          "portal-outputs/DetectionResults/CASE1234_10x_ph_bl.csv"
        ),
        "portal-outputs/Annotations" = c(
          "portal-outputs/Annotations/CASE123_10x_ph_bl.csv",
          "portal-outputs/Annotations/CASE1234_10x_ph_bl.csv"
        ),
        "portal-outputs/Confluency" = c(
          "portal-outputs/Confluency/CASE123_10x_ph_bl.csv",
          "portal-outputs/Confluency/CASE1234_10x_ph_bl.csv"
        ),
        "portal-outputs/Masks" = c(
          "portal-outputs/Masks/CASE123_10x_ph_bl_cp_masks.png",
          "portal-outputs/Masks/CASE1234_10x_ph_bl_cp_masks.png"
        ),
        character(0)
      )
    },
    .cellseg_s3_delete_key = function(key, config) {
      deleted <<- c(deleted, paste(config$bucket, key, sep = "::"))
      TRUE
    },
    .package = "cloneid",
    code = .deletep("CASE123", "s3://RIGHT_BUCKET/portal-inputs/", "s3://RIGHT_BUCKET/portal-outputs/")
  )

  expect_true(any(deleted == "RIGHT_BUCKET::portal-inputs/CASE123_10x_ph_bl.tif"))
  expect_true(any(deleted == "RIGHT_BUCKET::portal-outputs/Images/CASE123_10x_ph_bl_overlay.png"))
  expect_true(any(deleted == "RIGHT_BUCKET::portal-outputs/DetectionResults/CASE123_10x_ph_bl.csv"))
  expect_true(any(deleted == "RIGHT_BUCKET::portal-outputs/Annotations/CASE123_10x_ph_bl.csv"))
  expect_true(any(deleted == "RIGHT_BUCKET::portal-outputs/Confluency/CASE123_10x_ph_bl.csv"))
  expect_true(any(deleted == "RIGHT_BUCKET::portal-outputs/Masks/CASE123_10x_ph_bl_cp_masks.png"))

  expect_false(any(grepl("CASE1234_", deleted, fixed = TRUE)))
  expect_false(any(grepl("CASE12310x_", deleted, fixed = TRUE)))
  expect_false(any(grepl("CASE123_xx_", deleted, fixed = TRUE)))
  expect_false(any(grepl("OTHER_", deleted, fixed = TRUE)))
})

test_that("s3 durable artifact listing ignores prefix-collision and invalid keys", {
  mock_cfg <- list(
    backend = "s3",
    tmp = tempdir(),
    bucket = "cloneid4mysql8",
    inputPrefix = "inputs",
    outputPrefix = "outputs"
  )

  with_mocked_bindings(
    .cellseg_read_config = function() mock_cfg,
    .cellseg_s3_list_keys = function(prefix, config) {
      switch(prefix,
        "inputs" = c(
          "inputs/CASE123_10x_ph_bl.tif",
          "inputs/CASE1234_10x_ph_bl.tif",
          "inputs/CASE12310x_ph_bl.tif",
          "inputs/CASE123_xx_ph_bl.tif",
          "inputs/OTHER_10x_ph_bl.tif"
        ),
        "outputs/Images" = c(
          "outputs/Images/CASE123_10x_ph_bl_overlay.png",
          "outputs/Images/CASE1234_10x_ph_bl_overlay.png",
          "outputs/Images/CASE12310x_ph_bl_overlay.png",
          "outputs/Images/OTHER_10x_ph_bl_overlay.png"
        ),
        character(0)
      )
    },
    .package = "cloneid",
    code = {
      expect_identical(
        .list_input_artifacts("CASE123"),
        "s3://cloneid4mysql8/inputs/CASE123_10x_ph_bl.tif"
      )
      expect_identical(
        .list_output_artifacts("CASE123", "Images"),
        "s3://cloneid4mysql8/outputs/Images/CASE123_10x_ph_bl_overlay.png"
      )
    }
  )
})

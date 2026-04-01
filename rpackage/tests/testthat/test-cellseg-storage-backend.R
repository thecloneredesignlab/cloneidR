library(testthat)

.cfg     <- cloneid:::.cellseg_read_config
.paths   <- cloneid:::.cellseg_paths
.durable <- cloneid:::.cellseg_durable_paths
.tmpdirf <- cloneid:::.cellseg_tmp_dir
.is_s3   <- cloneid:::.cellseg_is_s3
.is_local<- cloneid:::.cellseg_is_local
.deletep <- cloneid:::.cellseg_delete_paths
.delete_artifacts <- cloneid:::.cellseg_delete_id_artifacts
.copy_in <- cloneid:::.cellseg_copy_to_input
.copy_out<- cloneid:::.cellseg_copy_to_output
.subs    <- cloneid:::.cellseg_output_subdirs
.stage_in<- cloneid:::.cellseg_stage_inputs_to_tmp
.clear_out <- cloneid:::.cellseg_clear_output_files
.list_out <- cloneid:::.cellseg_list_output_files
.list_mri_transient <- cloneid:::.cellseg_list_mri_transient_files
.validate_mri_transient <- cloneid:::.cellseg_validate_mri_transient_dir
.promote_mri <- cloneid:::.cellseg_promote_mri_files
.list_mri_masks <- cloneid:::.cellseg_list_promoted_mri_masks

test_that(".cellseg_read_config defaults backend to local when absent", {
  with_mocked_bindings(
    .read_yaml = NULL,
    .package = "cloneid",
    code = {
      mock_cfg <- list(cellSegmentation = list(
        input = "/tmp/input",
        output = "/tmp/output",
        tmp = "/tmp/tmp"
      ))

      result <- with_mocked_bindings(
        read_yaml = function(...) mock_cfg,
        .package = "yaml",
        code = .cfg()
      )

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

test_that(".cellseg_paths returns normalized local paths and rejects s3 backend", {
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

  expect_error(
    with_mocked_bindings(
      .cellseg_read_config = function() list(backend = "s3"),
      .package = "cloneid",
      code = .paths()
    ),
    "S3 cellSegmentation backend is configured"
  )
})

test_that(".cellseg_durable_paths rejects s3 backend but .cellseg_tmp_dir remains local-only", {
  s3_cfg <- list(
    backend = "s3",
    input = "/unused/input",
    output = "/unused/output",
    tmp = tempdir()
  )

  expect_error(
    .durable(s3_cfg),
    "S3 cellSegmentation backend is configured"
  )

  expect_identical(
    .tmpdirf(s3_cfg),
    normalizePath(s3_cfg$tmp)
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

  out_match <- file.path(outdir, "Images", "CASE123_overlay.png")
  out_other <- file.path(outdir, "Images", "OTHER_overlay.png")
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

  match_file <- file.path(outdir, "DetectionResults", "CASE123_pred.csv")
  other_file <- file.path(outdir, "DetectionResults", "OTHER_pred.csv")
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

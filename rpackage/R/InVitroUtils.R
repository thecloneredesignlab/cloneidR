# Internal helpers: generic DB fetch and exec.
# conn=NULL (default): open a fresh connection per call — conservative, matches existing
# package per-function lifecycle. Session-level reuse is deferred (pass conn= if needed).
.db_fetch <- function(stmt, conn = NULL) {
  if (is.null(conn)) conn <- connect2DB()
  rs <- dbSendQuery(conn, stmt)
  df <- fetch(rs, n = -1)
  dbClearResult(rs)
  df
}
.db_exec <- function(stmt, conn = NULL) {
  if (is.null(conn)) conn <- connect2DB()
  rs <- dbSendQuery(conn, stmt)
  dbClearResult(rs)
  invisible(NULL)
}


seed <- function(id, from, cellCount, flask, tx = Sys.time(), media=NULL, excludeOption=F, preprocessing=T, param=NULL, transactionId=0,
                 message_fn=NULL, input_fn=NULL){
  functionName <- as.character(match.call()[[1]])
  x=.seed_or_harvest(event = "seeding", id=id, from = from, cellCount = cellCount, tx = tx, flask = flask, media = media, excludeOption=excludeOption, preprocessing=preprocessing, param=param, transactionId=transactionId,
                     message_fn=message_fn, input_fn=input_fn)
  return(x)
}

harvest <- function(id, from, cellCount, tx = Sys.time(), media=NULL, excludeOption=F, preprocessing=T, param=NULL, transactionId=0,
                    message_fn=NULL, input_fn=NULL){
  functionName <- as.character(match.call()[[1]])
  x=.seed_or_harvest(event = "harvest", id=id, from=from, cellCount = cellCount, tx = tx, flask = NULL, media = media, excludeOption=excludeOption, preprocessing=preprocessing, param=param, transactionId=transactionId,
                     message_fn=message_fn, input_fn=input_fn)
  return(x)
}

inject <- function(mouseID, from, cellCount, tx = Sys.time(), strain, injection_type=23,
                   message_fn=NULL, input_fn=NULL, transactionId=0){
  functionName <- as.character(match.call()[[1]])
  x=.seed_or_harvest(event = "seeding", id=mouseID, from = from, cellCount = cellCount, tx = tx, flask = injection_type, media = strain, excludeOption=F, preprocessing=F, param=NULL, inject=injection_type,
                     message_fn=message_fn, input_fn=input_fn, transactionId=transactionId)
}

resect <- function(id, from, weight_mg, size_cubicmm, tx = Sys.time(), as='harvest',
                   message_fn=NULL, input_fn=NULL, transactionId=0){
  functionName <- as.character(match.call()[[1]])
  x=.seed_or_harvest(event = as, id=id, from=from, cellCount = size_cubicmm, tx = tx, flask = 'NULL', media = NULL, excludeOption=F, preprocessing=F, param=NULL, resect=weight_mg,
                     message_fn=message_fn, input_fn=input_fn, transactionId=transactionId)
  return(x)
}

diagnose <- function(event_id, patientID, path2segmentationresults, treatment=135, tx = Sys.time(),
                     message_fn=NULL, input_fn=NULL, transactionId=0){
  init(event_id, patientID, flask=23, media = treatment, preprocessing = F, as='seeding',
       path2segmentationresults=path2segmentationresults, cellCount=NULL, tx=tx,
       message_fn=message_fn, input_fn=input_fn, transactionId=transactionId)
}

follow_up <- function(event_id, diagnosis_id, treatment, path2segmentationresults, tx = Sys.time(),
                      message_fn=NULL, input_fn=NULL, transactionId=0){
  x=.seed_or_harvest(event = "harvest", id=event_id, from=diagnosis_id, cellCount = NULL, tx = tx, flask = NULL, media = treatment, preprocessing=F, param=NULL, path2segmentationresults=path2segmentationresults,
                     message_fn=message_fn, input_fn=input_fn, transactionId=transactionId)
  return(x)
}

init <- function(id, cellLine, cellCount, tx = Sys.time(), media=NULL, flask=NULL, preprocessing=T, as='harvest', path2segmentationresults=NULL,
                 message_fn=NULL, input_fn=NULL, transactionId=0){

  # Portal adapter closure — same pattern as .seed_or_harvest
  .msg <- if (is.null(message_fn)) function(...) invisible(NULL) else message_fn

  mydb = connect2DB()

  dish = list(dishCount=cellCount, cellSize="NULL", dishAreaOccupied="NULL")
  if (!is.null(path2segmentationresults)){
    ## Currently here we deal with MRI images exclusively:
    dish = try(.readMRISegmentationsOutput(id, path2segmentationresults))
    if (inherits(dish, "try-error")) {
      .msg("error", "MRISegmentation", conditionMessage(attr(dish, "condition")))
      dbDisconnect(mydb)
      if (is.null(message_fn)) stop(dish) else return(invisible(NULL))
    }
  }else if(!is.null(flask)){
    dishSurfaceArea_cm2 = .readDishSurfaceArea_cm2(flask, mydb)
    dish = .readCellSegmentationsOutput(id= id, from=cellLine, cellLine = cellLine, dishSurfaceArea_cm2 = dishSurfaceArea_cm2, cellCount = cellCount, preprocessing=preprocessing)
  }
  if(is.null(media)){
    media = "NULL"
  }
  if(is.null(flask)){
    flask = "NULL"
  }

  user = suppressWarnings(.db_fetch("SELECT user()", conn = mydb))[,1]

  if (transactionId == 0) {
    stmt = paste0("INSERT INTO Passaging ",
                  "(id, cellLine, event, date, cellCount, cellSize_um2, areaOccupied_um2, ",
                  "passage, flask, media, owner, lastModified) ",
                  "VALUES ('",id ,"', '",cellLine,"', '",as,"', '",as.character(tx),"', ",
                  dish$dishCount,", ", dish$cellSize,", ", dish$dishAreaOccupied,", ",
                  1,", ",flask,", ", media, ", '", user, "', '", user, "');")
  } else {
    stmt = paste0("INSERT INTO Passaging ",
                  "(id, cellLine, event, date, cellCount, cellSize_um2, areaOccupied_um2, ",
                  "passage, flask, media, owner, lastModified, transactionId) ",
                  "VALUES ('",id ,"', '",cellLine,"', '",as,"', '",as.character(tx),"', ",
                  dish$dishCount,", ", dish$cellSize,", ", dish$dishAreaOccupied,", ",
                  1,", ",flask,", ", media, ", '", user, "', '", user, "', ", transactionId, ");")
  }

  rs = try(.db_exec(stmt, conn = mydb))
  if (inherits(rs, "try-error")) {
    .msg("error", "DBInsert", conditionMessage(attr(rs, "condition")))
    dbDisconnect(mydb)
    if (is.null(message_fn)) stop(rs) else return(invisible(NULL))
  }

  dbDisconnect(mydb)
}


feed <- function(id, tx=Sys.time()){
  functionName <- as.character(match.call()[[1]])

  stmt = paste0("select * from Passaging where id = '",id,"'")
  kids = .db_fetch(stmt)

  ### Checks
  if (kids$event=="harvest"){
    print("Cannot feed cells that have already been harvested.", quote = F);
    return();
  }

  priorfeedings = kids[grep("feeding",names(kids),value=T)]
  ## Next un-occupied feeding index
  nextI = apply(!is.na(priorfeedings),1,sum)+1
  if(nextI>length(priorfeedings)){
    print(paste0("Cannot record more than ",length(priorfeedings)," feedings. Add additional feeding column first."), quote = F);
    return()
  }

  ### Update
  stmt = paste0("UPDATE Passaging SET ",names(priorfeedings)[nextI]," = '",as.character(tx),"' where id = '",id ,"'")
  .db_exec(stmt)
  print(paste("Feeding for",id,"recorded at",tx), quote = F)
}

## Read dishSurfaceArea_cm2 of this flask
.readDishSurfaceArea_cm2 <- function(flask, mydb = NULL){
  stmt = paste0("select dishSurfaceArea_cm2 from Flask where id = ", flask)
  dishSurfaceArea_cm2 = suppressWarnings(.db_fetch(stmt, conn = mydb))
  if(nrow(dishSurfaceArea_cm2)==0){
    print("Flask does not exist in database or its surface area is not specified")
    stopifnot(nrow(dishSurfaceArea_cm2)>0)
  }
  return(dishSurfaceArea_cm2[[1]])
}


getPedigreeTree <- function (cellLine = cellLine, id = NULL, cex = 0.5){
  library(ape)
  if (is.null(id)) {
    stmt = paste0("select * from Passaging where cellLine = '",
                  cellLine, "'")
    kids = suppressWarnings(.db_fetch(stmt))
  } else {
    kids = findAllDescendandsOf(id)
  }
  kids = kids[sort(kids$date, index.return = T)$ix, , drop = F]
  kids = kids[sort(kids$passage, index.return = T)$ix, , drop = F]
  rownames(kids) = kids$id
  .gatherDescendands <- function(kids, x) {
    ii = grep(paste0("^",x,"$"), kids$passaged_from_id1, ignore.case = T )
    if (isempty(ii)) {
      return("")
    }
    TREE_ = "("
    for (i in ii) {
      y = .gatherDescendands(kids, kids$id[i])
      if (nchar(y) > 0) {
        y = paste0(y, ":1,")
      }
      dx = kids$passage[i]
      TREE_ = paste0(TREE_, y, kids$id[i], ":", dx, ",")
    }
    TREE_ = gsub(",$", ")", TREE_)
    return(TREE_)
  }
  x = kids$id[1]
  TREE_ = .gatherDescendands(kids, x)
  TREE = paste0("(", TREE_, ":1,", x, ":1);")
  tr <- read.tree(text = TREE)
  str(tr)
  col = c("blue", "red")
  names(col) = c("seeding", "harvest")
  plot(tr, underscore = T, cex = cex, tip.color = col[kids[tr$tip.label, 
  ]$event])
  legend("topright", names(col), fill = col, bty = "n")
  return(tr)
}


findAllDescendandsOf <-function(ids, mydb = NULL, recursive = T, verbose = T){
  library(RMySQL)
  
  if(is.null(mydb)){
    mydb = connect2DB()
  }
  stmt = paste0("select * from Passaging where id IN ",paste0("('",paste0(ids, collapse = "', '"),"')  order by date DESC"));
  rs = suppressWarnings(RMySQL::dbSendQuery(mydb, stmt))
  parents = fetch(rs, n=-1)
  
  ## Recursive function to trace descendands
  .traceDescendands<-function(x){
    stmt = paste0("select * from Passaging where passaged_from_id1 = '",x,"'");
    rs = suppressWarnings(RMySQL::dbSendQuery(mydb, stmt))
    kids = fetch(rs, n=-1)
    out = kids$id
    if(recursive){
      for(id in kids$id){
        out = c(out, .traceDescendands(id))
      }
    }
    return(out)
  }
  
  ## Select statements, appending Ancestor
  alllineages = c()
  out = list();
  for(id in parents$id){
    d = c(id, .traceDescendands(id))
    d = setdiff(d, alllineages); ## exclude descendands with more recent parent (i.e. seedings)
    alllineages = c(alllineages, d)
    d = paste0("('",paste0(d, collapse = "', '"),"')")
    out[[id]] = paste0("select *, '",id,"' as Ancestor from Passaging where id IN ",d);
  }
  
  ## Union
  stmt = out[[1]]
  for(id in setdiff(names(out),names(out)[1])){
    stmt = paste0(stmt, " UNION (", out[[id]],")")
  }
  if(verbose){
    print(stmt)
  }
  
  ## Get results from DB
  rs = suppressWarnings(RMySQL::dbSendQuery(mydb, stmt))
  res = fetch(rs, n=-1)
  
  dbDisconnect(mydb)
  
  return(res)
}



readGrowthRate <- function(cellLine){
  cmd = paste0("select P2.*, P1.cellCount, P2.cellCount, DATEDIFF(P2.date, P1.date), POWER(P2.cellCount / P1.cellCount, 1 / DATEDIFF(P2.date, P1.date)) as GR_per_day",
               " FROM Passaging P1 JOIN Passaging P2",
               " ON P1.id = P2.passaged_from_id1",
               " WHERE P2.event='harvest' and P1.cellLine='",cellLine,"'")
  print(cmd, quote = F)
  return(.db_fetch(cmd))
}


populateLiquidNitrogenRacks <-function(rackID){
  library(RMySQL)
  mydb = connect2DB()
  for(box in 1:13){
    for (br in c('A','B','C','D','E','F','G','H','I')){
      for (bc in 1:9){
        cmd="INSERT INTO LiquidNitrogen (`Rack`, `Row`, `BoxRow`, `BoxColumn`)"
        cmd=paste0(cmd, " VALUES (",rackID,", ",box,", '",br,"', ",bc,");");
        print(cmd,quote = F)
        rs = dbSendQuery(mydb, cmd)
      }
    }
  }
  dbClearResult(dbListResults(mydb)[[1]])
  dbDisconnect(mydb)
}


plotCellLineHistory<-function(){
  kids = .db_fetch("select name, year_of_first_report, doublingTime_hours from CellLinesAndPatients where year_of_first_report >0;")
  kids = kids[sort(kids$year_of_first_report, index.return=T)$ix,]
  year = as.numeric(format(Sys.time(), "%Y"))
  par(mai = c(0.85,1,0.5,0.5))
  plot(c(kids$year_of_first_report[1], year), rep(1,2), type="l", ylim=c(0.5,nrow(kids)+0.5), xlab="year", ylab="", yaxt="n", col="blue")
  sapply(2:nrow(kids), function(i) lines(c(kids$year_of_first_report[i], year), rep(i,2), col="blue"))
  axis(2, at=1:nrow(kids), labels=kids$name, las=2)
  # sapply(1:nrow(kids), function(i) lines(c(2017,2018), rep(i,2), col="red", lwd=3))
  # legend("topleft", c("History of cell line", "Sc-Seq experiments"), fill=c("blue","red"))
  return(kids)
}

updateLiquidNitrogen <- function(id, cellCount, rack, row, boxRow, boxColumn){
  cmd=paste0("UPDATE LiquidNitrogen as x SET ",
             "x.id = '",id,"',",
             "x.cellCount = ",cellCount," WHERE ",
             "x.Rack = '",rack,"' AND ",
             "x.Row = '",row,"' AND ",
             "x.BoxRow = '",boxRow,"' AND ",
             "x.BoxColumn = '",boxColumn,"'");
  print(cmd)
  .db_exec(cmd)
}

removeFromLiquidNitrogen <- function(rack, row, boxRow, boxColumn){
  cmd=paste0("UPDATE LiquidNitrogen as x SET ",
             "x.id = NULL,",
             "x.cellCount = 0 WHERE ",
             "x.Rack = '",rack,"' AND ",
             "x.Row = '",row,"' AND ",
             "x.BoxRow = '",boxRow,"' AND ",
             "x.BoxColumn = '",boxColumn,"'");
  print(cmd)
  .db_exec(cmd)
}

plotLiquidNitrogenBox <- function (rack, row) {
  cmd = paste0("select * from LiquidNitrogen as x where ", "x.Rack = '",
               rack, "' AND ", "x.Row = '", row,"'")
  print(cmd)
  kids = .db_fetch(cmd)
  kids$id[is.na(kids$id)] = "NA"
  rc = apply(kids, 2, unique)
  par(mfrow = c(2, 2), mai = c(0, 0.5, 0.5, 0))
  plot(c(1, length(rc$BoxColumn)), c(1, length(rc$BoxRow)),
       col = "white", yaxt = "n", xaxt = "n", xlab = "", ylab = "",
       main = paste("Rack", rack, "; Row", row), ylim = rev(range(c(1,
                                                                    length(rc$BoxRow)))))
  axis(1, at = 1:length(rc$BoxColumn), labels = rc$BoxColumn,
       las = 1)
  axis(2, at = 1:length(rc$BoxRow), labels = rc$BoxRow, las = 2)
  cols = gray.colors(length(rc$id) * 1.2)[1:length(rc$id)]
  names(cols) = unique(rc$id)
  cols["NA"] = "white"
  for (i in 1:nrow(kids)) {
    points(match(kids$BoxColumn[i], rc$BoxColumn), match(kids$BoxRow[i],
                                                         rc$BoxRow), col = cols[kids$id[i]], pch = 20, cex = 4)
  }
  plot(1, 1, axes = F, col = "white")
  legend("topleft", names(cols), fill = cols)
}


.seed_or_harvest <- function(event, id, from, cellCount, tx, flask, media, excludeOption, preprocessing=T, param=NULL, inject=NULL, resect=NULL, path2segmentationresults=NULL, transactionId=0,
                             message_fn=NULL, input_fn=NULL){
  library(RMySQL)
  library(matlab)

  # ---- Portal adapter functions (default: no-op / readline) ----
  # message_fn(type, tag, value): portal logging; default is a no-op.
  .msg <- if (is.null(message_fn)) function(...) invisible(NULL) else message_fn
  # input_fn(inputOptions, prompt, retry_prompt, infomessage): user confirmation dialog.
  # Must return the selected option STRING from inputOptions.
  # Closed over by .wait_for_confirmation below (Option A: free variable from enclosing scope).
  # ---- End adapter setup ---------------------------------------------------

  .wait_for_confirmation <- function(CHECKRESULT, timeout = 10, prompt_template = "Error encountered while updating database: %s. No changes were made to the database. Type yes to confirm: ") {
    prompt_message <- sprintf(prompt_template, CHECKRESULT)
    if (!is.null(input_fn)) {
      # Portal context: delegate to injected input handler (input_fn from enclosing scope)
      input_fn(c("yes"), prompt_message, prompt_message, CHECKRESULT)
    } else {
      # Interactive context: readline with timeout
      confirmError <- "no"
      start_time <- Sys.time()
      while (confirmError != "yes" && as.numeric(difftime(Sys.time(), start_time, units = "secs")) < timeout) {
        Sys.sleep(0.5)
        confirmError <- readline(prompt = prompt_message)
      }
      if (confirmError != "yes") {
        stop("Operation timed out. No confirmation received.")
      }
    }
  }
  
  EVENTTYPES = c("seeding","harvest")
  otherevent = EVENTTYPES[EVENTTYPES!=event]
  
  mydb = connect2DB()
  
  stmt = paste0("select * from Passaging where id = '",from,"'");
  rs = suppressWarnings(dbSendQuery(mydb, stmt))
  parent = fetch(rs, n=-1)
  stmt = paste0("select * from Passaging where id = '",id,"'");
  rs = suppressWarnings(dbSendQuery(mydb, stmt))
  this = fetch(rs, n=-1)
  
  ### Checks
  CHECKRESULT="pass"
  if(nrow(this)>0){
    CHECKRESULT=paste(id,"already exists in table Passaging. Choose a different id.")
  }else if(nrow(parent)==0){
    CHECKRESULT=paste(from,"does not exist in table Passaging")
  }else if(parent$event !=otherevent){
    CHECKRESULT=paste(from,"is not a",otherevent,". You must do",event,"from a",otherevent)
  }else if(event=="seeding" && !is.na(parent$cellCount) && !is.na(cellCount) && cellCount>parent$cellCount){
    CHECKRESULT="You cannot seed more than is available from harvest!"
  }else{
    # Normalize "Parent" sentinel and empty string to NULL so they follow the
    # same inheritance path as a missing media argument.
    if (!is.null(media) && (media == "Parent" || media == "")) media <- NULL

    if(is.na(parent$media) || !is.null(media)){
      if(is.null(media)){
        CHECKRESULT="Please enter media information"
      }else{
        parent$media = media
      }
    }else{
      warning(paste("Copying media information from parent: media set to",parent$media))
    }
  }
  
  if (CHECKRESULT != "pass") {
    .msg("error", "validation", CHECKRESULT)
    dbDisconnect(mydb)
    stop(CHECKRESULT)
  }
  ## TODO: What if from is too far in the past
  
  ## flask cannot have changed if this is a harvest event: 
  if(event=="harvest"){
    flask = parent$flask
  }
  
  if(!is.null(inject)){
    dish =list(dishCount=cellCount, cellSize='NULL', dishAreaOccupied='NULL')
  }else if (!is.null(resect)){
    dish =list(dishCount='NULL', cellSize=cellCount, dishAreaOccupied=resect)
  }else if (!is.null(path2segmentationresults)){
    ## Currently here we deal with MRI images exclusively:
    print("Assuming MRI input")
    dish = try(.readMRISegmentationsOutput(id, path2segmentationresults))
  } else {
    dishSurfaceArea_cm2 = .readDishSurfaceArea_cm2(flask, mydb)
    ## @TODO: try-catch here
    dish = try(.readCellSegmentationsOutput(id= id, from=from, cellLine = parent$cellLine, dishSurfaceArea_cm2 = dishSurfaceArea_cm2, cellCount = cellCount, excludeOption=excludeOption, preprocessing=preprocessing, param=param));
  }
  
  if(class(dish)=="try-error"){
    .wait_for_confirmation("", prompt_template = "Error encountered during segmentation. No changes are made to the database. Type yes to confirm: ", timeout = 10)
    return()
  }
  
  ### Passaging info
  passage = parent$passage
  if(event=="seeding"){
    passage = passage+1
  }
  
  ## User info
  mydb = connect2DB()
  rs = suppressWarnings(dbSendQuery(mydb, "SELECT user()"));
  user=fetch(rs, n=-1)[,1];
  
  ### Check id, passaged_from_id1: is there potential for incorrect assignment between them?
  stmt = "SELECT id, event, passaged_from_id1, correctedCount,passage, date from Passaging";
  rs = suppressWarnings(dbSendQuery(mydb, stmt))
  passaging = fetch(rs, n=-1)
  rownames(passaging) <- passaging$id
  passaging$passage_id <- sapply(passaging$id, .unique_passage_id)
  passaging = passaging[!is.na(passaging$passage_id),]
  # x=data.table::transpose(as.data.frame(c(id , event, from, dish$dishCount, passage)))
  # colnames(x) = c("id", "event", "passaged_from_id1", "correctedCount", "passage")
  if(transactionId == 0){
    x=data.table::transpose(as.data.frame(c(id ,parent$cellLine,from,event,as.character(tx),dish$dishCount,dish$dishCount,dish$cellSize, dish$dishAreaOccupied, passage,flask,parent$media,  user,  user)))
    colnames(x) = c("id", "cellLine","passaged_from_id1", "event", "date", "cellCount","correctedCount","cellSize_um2","areaOccupied_um2", "passage", "flask", "media", "owner", "lastModified")
  }else{
    x=data.table::transpose(as.data.frame(c(id ,parent$cellLine,from,event,as.character(tx),dish$dishCount,dish$dishCount,dish$cellSize, dish$dishAreaOccupied, passage,flask,parent$media,  user,  user, transactionId)))
    colnames(x) = c("id", "cellLine","passaged_from_id1", "event", "date", "cellCount","correctedCount","cellSize_um2","areaOccupied_um2", "passage", "flask", "media", "owner", "lastModified", "transactionId")
  }
  rownames(x) <- x$id
  x4DB <- x
  x$passage_id <- .unique_passage_id(x$id)
  probable_ancestor <- try(.assign_probable_ancestor(x$id,xi=passaging), silent = T)
  ancestorCheck = T;
  if (!inherits(probable_ancestor, "try-error") && !isempty(probable_ancestor)) {
    x$probable_ancestor = probable_ancestor
    if(x$passaged_from_id1!=x$probable_ancestor){
      # confirmAncestorCorrect = ""
      # while(!confirmAncestorCorrect %in% c("yes", "no")){
        # confirmAncestorCorrect <- readline(prompt=paste0("Warning encountered while updating database: Was ",x$id," really derived from ",x$passaged_from_id1,"? type yes/no: "))
      # }
      # if(confirmAncestorCorrect=="no"){
        # ancestorCheck=F;
        # .wait_for_confirmation("", prompt_template = "No changes are made to the database. Please modify passaged_from_id1, then rerun. Type yes to confirm: ", timeout = 10)
      # }
      prompt= glue::glue(
        "\n⚠️  {x$id} appears to either not follow the naming convention or  have the wrong parent.",
        "\n❌  Aborting update: please correct  'id' or 'passaged_from_id1' ",
        "to follow the naming convention and rerun the script."
      )
      print(prompt)
      ancestorCheck=F;
      .wait_for_confirmation("", prompt_template = "No changes are made to the database. Type yes to confirm: ", timeout = 10)
    }
  }
  
  ## non-numeric entries formatting:
  ii=which(!names(x) %in% c("cellSize_um2","areaOccupied_um2","correctedCount","cellCount", "passage", "flask", "media"))
  x[ii]=paste0("'",x[ii],"'")
  x4DB <- x[names(x4DB)]
  x4DB[is.na(x4DB)]="NULL"
  
  ## Attempt to update the DB:
  if(ancestorCheck){
    # # stmt = paste0("INSERT INTO Passaging (id, passaged_from_id1, event, date, cellCount, passage, flask, media, owner, lastModified, lastModifiedDate) ",
    # # "VALUES ('",id ,"', '",from,"', '",event,"', '",as.character(tx),"', ",dish$dishCount,", ", passage,", ",flask,", ", parent$media, ", '", user, "', '", user, "', NOW());")
    ### Insert
    stmt = paste0("INSERT INTO Passaging (",paste(names(x4DB), collapse = ", "),") ",
                  "VALUES (",paste(x4DB, collapse = ", "),");")
    rs = try(dbSendQuery(mydb, stmt))
    if(class(rs)!="try-error"){
      stmt = paste0("update Passaging set correctedCount = ",x4DB$correctedCount," where id='",id,"';")
      rs = dbSendQuery(mydb, stmt)
      
      stmt = paste0("update Passaging set areaOccupied_um2 = ",x4DB$areaOccupied_um2," where id='",id,"';")
      rs = dbSendQuery(mydb, stmt)
      stmt = paste0("update Passaging set cellSize_um2 = ",x4DB$cellSize_um2," where id='",id,"';")
      rs = dbSendQuery(mydb, stmt)
      stmt = paste0("update Passaging set lastModified = ",x4DB$lastModified," where id='",id,"';")
      rs = dbSendQuery(mydb, stmt)
    }else{

      .wait_for_confirmation("", prompt_template = "Error encountered while updating database: no changes were made to the database. Please check id is not redundant with existing IDs, then rerun. Type yes to confirm: ", timeout = 30)
    }
  }
  
  try(dbClearResult(dbListResults(mydb)[[1]]), silent = T)
  try(dbDisconnect(mydb), silent = T)
  
  return(x4DB)
}

#' Find unique passage identifier in an event id string.
#'
#' @param i Event id.
#' @param idx_only If 1, returns index in split; if 2, returns passage id string; else full id.
#' @param to_numeric If TRUE and idx_only=2, returns passage number as numeric.
.unique_passage_id <- function(i, idx_only=0, to_numeric=F){
  vec=unlist(strsplit(i,split="_"))
  idx=grep("A[0-9]{1,2}", vec, value=F)
  if(length(idx)==1){
    if(idx_only==1){
      return(idx)
    } else if(idx_only==2){
      if(to_numeric){
        return(as.numeric(.strip_non_digit_ends(vec[idx])))
      }
      return(vec[idx])
    }
    out=paste(head(vec,idx),collapse="_")
    return(.strip_after_last_digit(out))
  } else{
    return(NA)
  }
}

#' Suggests the probable ancestor for a given event id, based on passage structure.
#'
#' @param i Event id to check.
#' @param xi Full data.frame of events.
#' @return The suggested ancestor id.
.assign_probable_ancestor <- function(i, xi){
  assigned_ancestor = xi$passaged_from_id1[xi$id==i]
  if(xi$event[xi$id==i]=="harvest"){
    target_passage = xi$id[xi$passage_id==xi$passage_id[xi$id==i] & xi$event=="seeding"]
    if(assigned_ancestor %in% target_passage){
      return(assigned_ancestor)
    }
    target_passage = target_passage[xi[target_passage,'date'] < xi[i,'date']]
    d = adist(i, target_passage)
    if(sum(as.numeric(d==min(d))) > 1){
      warning(paste0("Ambiguous probable_ancestor (seeding) for ",i), immediate. = T)
    } else if(length(target_passage) == 0){
      print(paste("No probable ancestor seed assignable for harvest", i))
    }
    return(target_passage[which.min(d)])
  }
  if(xi$event[xi$id==i]=="seeding"){
    passage_split <- unlist(strsplit(i, split="_"))
    idx = .unique_passage_id(i, idx_only=1)
    passage_id = .strip_after_last_digit(passage_split[idx])
    passage_id = paste0("A", as.numeric(gsub("A", "", passage_id)) - 1)
    passage_id <- paste0(c(passage_split[1:(idx-1)], passage_id), collapse="_")
    target_passage <- xi[xi$passage_id==passage_id & xi$event=='harvest',]
    if(assigned_ancestor %in% target_passage$id){
      return(assigned_ancestor)
    }
    target_passage = target_passage[xi[target_passage$id,'date'] < xi[i,'date'],]
    if(nrow(target_passage) > 1){
      warning(paste0("Ambiguous probable_ancestor (harvest) for ", i), immediate. = T)
    } else if(nrow(target_passage) == 0){
      print(paste("No probable ancestor harvest assignable for seed", i))
    }
    return(target_passage$id[which.max(as.Date(target_passage$date))])
  }
}


#' Retain everything up to and including the last digit in a string.
.strip_after_last_digit <- function(x) {
  sub("(.*\\d).*", "\\1", x)
}

#' Remove all characters before the first digit and after the last digit.
.strip_non_digit_ends <- function(x) {
  out <- regmatches(x, gregexpr("\\d+", x))
  sapply(out, function(digits) if(length(digits)) paste0(digits, collapse="") else NA)
}

# ---- Per-id artifact removal from global INDIR/OUTDIR -----------------------
# Deletes all files in global INDIR/OUTDIR matching the unified ingest regex
# ^{id}_([0-9]+x_ph|t1|t2|flair|pd) (covers microscopy TIFs and MRI NIfTIs) and all
# files in each of the five OUTDIR subfolders matching ^{id}_. Does NOT delete
# directories. Safe to call when stores are empty (no-op). Called by the portal's
# IVU.R caller via cloneid:::.remove_id_artifacts() for compensating rollback.
.remove_id_artifacts <- function(id, indir, outdir) {
  del_in <- list.files(indir, pattern = paste0("^", id, "_([0-9]+x_ph|t1|t2|flair|pd)"), full.names = TRUE)
  del_in <- grep("\\.tif$", del_in, value = TRUE, ignore.case = TRUE)
  if (length(del_in) > 0) file.remove(del_in)
  for (sub in c("DetectionResults", "Annotations", "Images", "Confluency", "Masks")) {
    del_out <- list.files(file.path(outdir, sub),
                          pattern = paste0("^", id, "_([0-9]+x_ph|t1|t2|flair|pd)"), full.names = TRUE)
    if (length(del_out) > 0) file.remove(del_out)
  }
  invisible(NULL)
}

# Returns the canonical cell-segmentation paths from the package config.
# Single parse point: both InVitroUtils.R and IVU.R use this helper
# (IVU via cloneid:::.cellseg_paths()) so the YAML is never read in two places.
.cellseg_paths <- function() {
  yml <- yaml::read_yaml(paste0(system.file(package = 'cloneid'), '/config/config.yaml'))
  list(
    input  = paste0(normalizePath(yml$cellSegmentation$input),  "/"),
    output = paste0(normalizePath(yml$cellSegmentation$output), "/"),
    tmp    = normalizePath(yml$cellSegmentation$tmp)
  )
}

.readCellSegmentationsOutput <- function(id, from, cellLine, dishSurfaceArea_cm2, cellCount, excludeOption, preprocessing=T, param=NULL){
  ## Typical values for dishSurfaceArea_cm2 are:
  ## a) 75 cm^2 = 10.1 cm x 7.30 cm
  ## b) 25 cm^2 = 5.08 cm x 5.08 cm
  ## c) well from 96-plate = 0.32 cm^2
  ## CellSegmentations Settings; @TODO: should be set under settings, not here
  UM2CM = 1e-4
  .cellseg <- .cellseg_paths()
  TMP_DIR = .cellseg$tmp
  unlink(TMP_DIR,recursive=T, force = T)
  TMP_DIR = paste0(TMP_DIR,filesep,id);
  unlink(TMP_DIR,recursive=T, force = T)
  CELLSEGMENTATIONS_OUTDIR = .cellseg$output
  CELLSEGMENTATIONS_INDIR  = .cellseg$input
  # QUPATH_PRJ = "~/Downloads/qproject/project.qpproj"
  # QSCRIPT = "~/Downloads/qpscript/runDetectionROI.groovy"
  CELLPOSE_PARAM=paste0(find.package("cloneid"),filesep,"python/cellPose.param")
  PYTHON_SCRIPTS=list.files(paste0(find.package("cloneid"),filesep,"python"), pattern=".py", full.names = T)
  CELLPOSE_SCRIPT=grep("GetCount_cellPose.py",PYTHON_SCRIPTS, value = T)
  PREPROCESS_SCRIPT=grep("preprocessing.py",PYTHON_SCRIPTS, value = T)
  TISSUESEG_SCRIPT=grep("tissue_seg.py",PYTHON_SCRIPTS, value = T)
  QCSTATS_SCRIPT=grep("QC_Statistics.py",PYTHON_SCRIPTS, value = T)
  suppressWarnings(dir.create(paste0(CELLSEGMENTATIONS_OUTDIR,"DetectionResults")))
  suppressWarnings(dir.create(paste0(CELLSEGMENTATIONS_OUTDIR,"Annotations")))
  suppressWarnings(dir.create(paste0(CELLSEGMENTATIONS_OUTDIR,"Images")))
  suppressWarnings(dir.create(paste0(CELLSEGMENTATIONS_OUTDIR,"Confluency")))
  suppressWarnings(dir.create(paste0(CELLSEGMENTATIONS_OUTDIR,"Masks"))) 
  # suppressWarnings(dir.create("~/Downloads/qpscript"))
  # suppressWarnings(dir.create(fileparts(QUPATH_PRJ)$pathstr))
  # qpversion = list.files("/Applications", pattern = "QuPath")
  # qpversion = gsub(".app","", gsub("QuPath","",qpversion))
  # qpversion = qpversion[length(qpversion)]
  
  ## Load environment and source python scripts
  LOADEDENV='cellpose' %in% reticulate::conda_list()$name
  if(LOADEDENV){
    reticulate::use_condaenv("cellpose")
    # py_config()
    print('Cellpose environment loaded')
    # use_condaenv("cellpose", required = TRUE)
    sapply(PYTHON_SCRIPTS, reticulate::source_python)
  }
  
  ## Copy raw images to temporary directory:
  dir.create(TMP_DIR, recursive = T)
  f_i = list.files(CELLSEGMENTATIONS_INDIR, pattern = paste0("^",id,"_"), full.names = T)
  f_i = grep("x_ph_",f_i,value=T)
  f_i = grep(".tif$",f_i,value=T)
  file.copy(f_i, TMP_DIR)
  ## Delete output files from prior runs (anchored per-id pattern; all 5 subfolders):
  for(subfolder in c("Annotations","Images","DetectionResults","Confluency","Masks")){
    f = list.files(paste0(CELLSEGMENTATIONS_OUTDIR,subfolder), pattern = paste0("^",id,"_([0-9]+x_ph|t1|t2|flair|pd)"), full.names = T)
    file.remove(f)
  }
  ## Input files should never be deleted. What goes into `CELLSEGMENTATIONS_OUTDIR` stays in CELLSEGMENTATIONS_OUTDIR. This will ensure that raw data is never deleted in case any analysis needs to be redone
  ## @TODO: To prevent accumulation of incompletely processed images in CELLSEGMENTATIONS_OUTDIR, we would want to copy them over only after we ensure correct processing.
  
  ## Preprocessing
  if(preprocessing){
    for(x in list.files(TMP_DIR, pattern = ".tif", full.names = T)){
      # cmd = paste("python3",PREPROCESS_SCRIPT, x, cellLine)
      # system(cmd)
      print(paste("Using", PREPROCESS_SCRIPT))
      reticulate::source_python(PREPROCESS_SCRIPT)
      ApplyGammaCorrection(x, cellLine)
    }
  }
  
  ## Cell segmentation
  ## Call CellPose for images inside temp dir 
  # virtualenv_list()
  source_python(CELLPOSE_SCRIPT)
  ## cellPose parameters:
  if(is.null(param)){
    cpp=read.table(CELLPOSE_PARAM,header=T,row.names = 1) 
    ##Let's zoom in on just a subset of entries of cpp, based on the "cellLine" param
    stmt = paste0("select id, cellLine from Passaging where id in ('",paste(setdiff(rownames(cpp),"default"),collapse = "', '"),"') ")
    cli = .db_fetch(stmt)
    cli=rbind(cli,c("default","default"))
    rownames(cli)=cli$id
    cpp$cellLine=cli[rownames(cpp),"cellLine"]
    cpp=cpp[cpp$cellLine %in% c(cellLine,"default"),,drop=F]
    ## Now iterate through entries of cpp that we have left:
    for(cl in setdiff(rownames(cpp),"default")){
      tmp=cloneid::findAllDescendandsOf(id=cl,verbose = F); 
      if(from %in% tmp$id){
        param=as.list(cpp[cl,])
        ##@TODO: this will pick the first lineage that fits. If there are multiple lineages that fit, the subsequent ones will be ignored
        break; 
      }
    }
    if(is.null(param)){
      param=as.list(cpp["default",])
    }
  }
  param$cellposeModel=paste0(find.package("cloneid"),filesep,"python",filesep, param$cellposeModel)
  print(param)
  run(TMP_DIR,normalizePath(param$cellposeModel),TMP_DIR,".tif", as.character(param$diameter), as.character(param$flow_threshold), as.character(param$cellprob_threshold))
  
  ## Tissue segmentation
  for(x in f_i){
    imgPath=paste0(TMP_DIR,filesep,fileparts(x)$name,".tif")
    reticulate::source_python(TISSUESEG_SCRIPT)
    get_mask(imgPath,paste0(TMP_DIR,filesep,"Confluency"),toupper(cellLine),"False")
  }
  
  ## Add QC statistics
  if(LOADEDENV){
    reticulate::source_python(QCSTATS_SCRIPT)
    QC_Statistics(TMP_DIR,paste0(TMP_DIR,filesep,"cellpose_count"),'.tif')
  }
  
  ## Move files from tempDir to destination:
  .move_temp_files(TMP_DIR)
  
  ## Wait and look for imaging analysis output
  ia=.wait_for_analysis_output(id, length(f_i))
  
  ## Read automated image analysis output
  cellCounts = matrix(NA,length(ia$f),4);
  colnames(cellCounts) = c("areaCount","area_cm2","dishAreaOccupied", "cellSize_um2")
  rownames(cellCounts) = sapply(ia$f, function(x) fileparts(x)$name)
  # pdf(OUTSEGF)
  for(i in 1:length(ia$f)){
    dm = read.table(ia$f[i],sep="\t", check.names = F, stringsAsFactors = F, header = T)
    colnames(dm)[grep("^Area",colnames(dm))]="Cell: Area"; ## Replace cellPose column name -- @TODO: saeed fix directly in cellposeScript
    anno = read.table(ia$f_a[i],sep="\t", check.names = T, stringsAsFactors = F, header = T)
    conf = read.csv(ia$f_c[i])
    colnames(anno) = tolower(colnames(anno))
    areaCount = nrow(dm)
    # areaCount = sum(conf$`Area.in.um`)/median(dm$`Cell: Area`)
    area_cm2 = anno[1,grep("^area.",colnames(anno))]*UM2CM^2
    cellCounts[fileparts(ia$f[i])$name,] = c(areaCount, area_cm2, sum(conf$`Area.in.um`), quantile(dm$`Cell: Area`, 0.9, na.rm=T))
    # ## Visualize
    # ## @TODO: Delete
    # if(!file.exists(ia$f_o[i])){
    #   la=raster::raster(f_i[i])
    #   ROI <- as(raster::extent(100, 1900, la@extent@ymax - 1200, la@extent@ymax - 100), 'SpatialPolygons')
    #   la_ <- raster::crop(la, ROI)
    #   raster::plot(la_, ann=FALSE,axes=FALSE, useRaster=T,legend=F)
    #   mtext(fileparts(f_i[i])$name, cex=1)
    #   points(dm$`Centroid X µm`,la@extent@ymax - dm$`Centroid Y µm`, col="black", pch=20, cex=0.3)
    # }else{
    #   img <- magick::image_read(ia$f_o[i])
    #   plot(img)
    #   mtext(fileparts(ia$f_o[i])$name, cex=1)
    # }
  }
  # dev.off()
  # file.copy(OUTSEGF, paste0(TMP_DIR,filesep) )
  
  ## Predict cell count error
  print("Predicting cell count error...",quote=F)
  for(i in 1:length(ia$f_a)){
    anno = read.table(ia$f_a[i],sep="\t", check.names = T, stringsAsFactors = F, header = T)
    ## No cells detected
    if(is.null(anno$Num.Detections)){
      anno$Num.Detections=0;
    }
    ## use CL-specific model if it exists, otherwise use general model
    data(list="General_logErrorModel")
    ## Loads cell line specific linear model "linM" -- overrides general model loaded above if cell line specific model exists
    data(list=paste0(cellLine,"_logErrorModel"))
    if(!any(c("Variance.of.Laplician","fft") %in% colnames(anno))){
      warning("No features for error prediction available", immediate. = T)
      excludeOption=T
      break;
    }
    anno$log.error = predict(linM, newdata=anno)
    if(anno$log.error>linM$MAXERROR){
      warning("Low image quality predicted for at least one image")
      excludeOption=T
      break;
    }else{
      print(paste("Cell count error predicted as negligible for",ia$f_a[i]),quote=F)
    }
  }
  
  ## Provide option to exclude subset of images
  if(excludeOption){
    toExclude <- readline(prompt="Exclude any images (bl, br, tl, tr)?")
    if(nchar(toExclude)>0){
      toExclude = sapply(strsplit(toExclude,",")[[1]],trimws)
      toExclude = c(paste0(as.character(toExclude),".tif"), paste0(as.character(toExclude),"$"))
      ii = sapply(toExclude, function(x) grep(x, rownames(cellCounts)))
      ii = unlist(ii[sapply(ii,length)>0])
      if(!isempty(ii)){
        print(paste("Excluding",rownames(cellCounts)[ii],"from analysis."), quote = F)
        cellCounts= cellCounts[-ii,, drop=F]
      }
      if(length(ii)==length(ia$f)){
        stop("At least one valid image needs to be left. Aborting")
      }
    }
  }
  
  
  ## Calculate cell count per dish
  area2dish = dishSurfaceArea_cm2 / sum(cellCounts[,"area_cm2"])
  dishCount = round(sum(cellCounts[,"areaCount"]) * area2dish)
  dishConfluency = sum(cellCounts[,"dishAreaOccupied"]) * area2dish
  cellSize = median(cellCounts[,"cellSize_um2"],na.rm=T)
  print(paste("Estimated number of cells in entire flask at",dishCount), quote = F)
  
  if(!is.na(cellCount) && (dishCount/cellCount > 2 || dishCount/cellCount <0.5)){
    warning(paste0("Automated image analysis deviates from input cell count by more than a factor of 2. CellCount set to the former (",dishCount," cells)"))
  }
  return(list(dishCount=dishCount,dishAreaOccupied=dishConfluency, cellSize=cellSize))
}


# MRI-specific file promotion: copies raw input → global INDIR, mask + optional
# cavity → global OUTDIR/Images. No CSV/PNG parity checks (MRI has none).
# All deposited filenames match the unified ingest regex so .remove_id_artifacts
# handles compensating rollback correctly.
.move_mri_files <- function(TMP_DIR, id) {
  .cellseg <- .cellseg_paths()
  mri_regex <- paste0("^", id, "_(t1|t2|flair|pd)")
  msk <- list.files(TMP_DIR, pattern = paste0(mri_regex, ".*_msk\\.nii"),  full.names = TRUE)
  cav <- list.files(TMP_DIR, pattern = paste0(mri_regex, ".*_cavity\\.nii"), full.names = TRUE)
  raw <- list.files(TMP_DIR, pattern = paste0(mri_regex, "\\.nii"),          full.names = TRUE)
  # raw must not match mask or cavity files
  raw <- raw[!grepl("_msk\\.nii|_cavity\\.nii", raw)]
  if (length(msk) != 1)
    stop(paste0("Expected exactly 1 mask NIfTI for id ", id, "; found ", length(msk)))
  if (length(raw) != 1)
    stop(paste0("Expected exactly 1 raw NIfTI for id ", id, "; found ", length(raw)))
  file.copy(raw[1], .cellseg$input)
  file.copy(msk[1], paste0(.cellseg$output, "Images/"))
  if (length(cav) == 1)
    file.copy(cav[1], paste0(.cellseg$output, "Images/"))
}

.readMRISegmentationsOutput <- function(id, path2segmentationresults) {
  ingest_regex <- paste0("^", id, "_([0-9]+x_ph|t1|t2|flair|pd)")
  f_i <- list.files(path2segmentationresults, pattern = ingest_regex, full.names = TRUE)
  if (length(f_i) != length(list.files(path2segmentationresults)))
    stop(paste0("All files in (", path2segmentationresults,
                ") must match ingest regex for id: ", id))
  .move_mri_files(path2segmentationresults, id)
  msk_promoted <- list.files(paste0(.cellseg_paths()$output, "Images/"),
                              pattern = paste0("^", id, "_(t1|t2|flair|pd).*_msk\\.nii"),
                              full.names = TRUE)
  if (length(msk_promoted) == 0)
    stop(paste0("Mask not found in OUTDIR/Images after promotion for id: ", id))
  nii    <- RNifti::readNifti(msk_promoted[1])
  volume <- sum(as.numeric(nii))
  return(list(dishCount = "NULL", cellSize = volume, dishAreaOccupied = "NULL"))
}


.move_temp_files <- function(TMP_DIR, segmentationRegex="overlay.", moveSegmentationInputToo=F) {
  .cellseg <- .cellseg_paths()
  CELLSEGMENTATIONS_OUTDIR <- .cellseg$output
  CELLSEGMENTATIONS_INDIR  <- .cellseg$input

  cellPoseOut_img <- list.files(TMP_DIR, recursive = TRUE, pattern = segmentationRegex, full.names = TRUE)
  cellPoseIn_img  <- gsub(segmentationRegex, '.', cellPoseOut_img, fixed = TRUE)
  N <- length(cellPoseIn_img)

  ## Move files from TMP_DIR to destination directories
  cellPoseOut_csv <- list.files(TMP_DIR, recursive = TRUE, pattern = ".csv", full.names = TRUE)
  f   <- grep("pred",          cellPoseOut_csv, value = TRUE)
  f_a <- grep("cellpose_count",cellPoseOut_csv, value = TRUE)
  f_c <- grep("Confluency",    cellPoseOut_csv, value = TRUE)
  f_m <- list.files(TMP_DIR, recursive = TRUE, pattern = "mask", full.names = TRUE)
  cellPoseMsk  <- grep("_cp_masks\\.png$", f_m, value = TRUE)
  tissueSegMsk <- grep("_cp_masks\\.png$", f_m, value = TRUE, invert = TRUE)
  # Count check uses cellPoseMsk (one per input image), not f_m which also includes
  # tissue-segmentation _mask.png files from the Confluency subfolder.
  if(length(f)!=N || length(f_a)!=N || length(f_c)!=N || length(cellPoseMsk)!=N){
    warning("No results were kept because unexpected number of output files were detected. Likely an error was encountered while processing at least one image.")
    return()
  }

  sapply(f,            function(x) file.copy(x, paste0(CELLSEGMENTATIONS_OUTDIR, "DetectionResults")))
  sapply(f_a,          function(x) file.copy(x, paste0(CELLSEGMENTATIONS_OUTDIR, "Annotations")))
  sapply(f_c,          function(x) file.copy(x, paste0(CELLSEGMENTATIONS_OUTDIR, "Confluency")))
  sapply(cellPoseMsk,  function(x) file.copy(x, paste0(CELLSEGMENTATIONS_OUTDIR, "Masks")))
  sapply(tissueSegMsk, function(x) file.copy(x, paste0(CELLSEGMENTATIONS_OUTDIR, "Confluency")))
  sapply(cellPoseOut_img, function(x) file.copy(x, paste0(CELLSEGMENTATIONS_OUTDIR, "Images")))

  if(moveSegmentationInputToo){
    sapply(cellPoseIn_img, function(x) file.copy(x, CELLSEGMENTATIONS_INDIR))
  }
}


.wait_for_analysis_output <- function(id, howMany) {
  CELLSEGMENTATIONS_OUTDIR <- .cellseg_paths()$output

  ## Wait and look for imaging analysis output
  print(paste0("Waiting for ", id, " to appear under ", CELLSEGMENTATIONS_OUTDIR, " ..."), quote = FALSE)
  f_o <- c()
  start_time <- Sys.time()
  timeout <- 120  # 2 minutes in seconds
  while (length(f_o) < howMany && as.numeric(difftime(Sys.time(), start_time, units = "secs")) < timeout) {
    Sys.sleep(3)
    f_o <- list.files(paste0(CELLSEGMENTATIONS_OUTDIR, "Images"), pattern = paste0("^",id, "_([0-9]+x_ph|t1|t2|flair|pd)"), full.names = TRUE)
  }

  if (length(f_o) < howMany) {
    warning("Timed out waiting for analysis output.")
    return()
  }

  f   <- list.files(paste0(CELLSEGMENTATIONS_OUTDIR, "DetectionResults"), pattern = paste0("^",id, "_([0-9]+x_ph|t1|t2|flair|pd)"), full.names = TRUE)
  f_a <- list.files(paste0(CELLSEGMENTATIONS_OUTDIR, "Annotations"),      pattern = paste0("^",id, "_([0-9]+x_ph|t1|t2|flair|pd)"), full.names = TRUE)
  f_c <- list.files(paste0(CELLSEGMENTATIONS_OUTDIR, "Confluency"),        pattern = paste0("^",id, "_([0-9]+x_ph|t1|t2|flair|pd)"), full.names = TRUE)
  f_c <- grep(".csv", f_c, value = TRUE)
  print(paste0("Output found for ", fileparts(f_o[1])$name, " and ", (length(f_o) - 1), " other image files."), quote = FALSE)
  return(list(f = f, f_a = f_a, f_o = f_o, f_c = f_c))
}

getMRIdata<-function(id, signal="t2"){
  .cellseg <- .cellseg_paths()
  CELLSEGMENTATIONS_OUTDIR <- .cellseg$output
  CELLSEGMENTATIONS_INDIR  <- .cellseg$input
  x=list.files(paste0(CELLSEGMENTATIONS_OUTDIR,"/Images"), pattern=paste0("^",id,"_",signal), full.names = T)[1]
  nii_mask=RNifti::readNifti(x)
  signal=gsub("_cavity","",signal)
  x=list.files(CELLSEGMENTATIONS_INDIR, pattern=paste0("^",id,"_",signal), full.names = T)[1]
  nii=RNifti::readNifti(x)
  return(list(nii=nii,mask=nii_mask))
}


.QuPathScript <- function(qpdir, cellLine){
  # Standard pipeline:
  runPlugin = "runPlugin('qupath.imagej.detect.cells.WatershedCellDetection', '{\"detectionImageBrightfield\": \"Hematoxylin OD\",  \"requestedPixelSizeMicrons\": 1.0,  \"backgroundRadiusMicrons\": 15.0,  \"medianRadiusMicrons\": 0.0,  \"sigmaMicrons\": 1.5,  \"minAreaMicrons\": 2.0,  \"maxAreaMicrons\": 1000.0,  \"threshold\": 0.1,  \"maxBackground\": 2.9,  \"watershedPostProcess\": false,  \"cellExpansionMicrons\": 2.5,  \"includeNuclei\": false,  \"smoothBoundaries\": true,  \"makeMeasurements\": true}');"
  # # NCI-N87 pipeline:
  if(cellLine=="NCI-N87"){
    # runPlugin = "runPlugin('qupath.imagej.detect.cells.WatershedCellDetection', '{\"detectionImageBrightfield\": \"Hematoxylin OD\",  \"requestedPixelSizeMicrons\": 0.5,  \"backgroundRadiusMicrons\": 8.0,  \"medianRadiusMicrons\": 0.0,  \"sigmaMicrons\": 1.5,  \"minAreaMicrons\": 40.0,  \"maxAreaMicrons\": 400.0,  \"threshold\": 0.09,  \"maxBackground\": 3.0,  \"watershedPostProcess\": false,  \"cellExpansionMicrons\": 5.0,  \"includeNuclei\": false,  \"smoothBoundaries\": true,  \"makeMeasurements\": true}');"
    runPlugin = "runPlugin('qupath.imagej.detect.cells.WatershedCellDetection', '{\"detectionImageBrightfield\": \"Hematoxylin OD\",  \"requestedPixelSizeMicrons\": 0.461,  \"backgroundRadiusMicrons\": 12.0,  \"medianRadiusMicrons\": 0.0,  \"sigmaMicrons\": 1,  \"minAreaMicrons\": 15,  \"maxAreaMicrons\": 200.0,  \"threshold\": 0.2,  \"maxBackground\": 3.0,  \"watershedPostProcess\": true,  \"cellExpansionMicrons\": 3.0,  \"includeNuclei\": false,  \"smoothBoundaries\": true,  \"makeMeasurements\": true}');"
  }else if(cellLine=="HGC-27" || cellLine=="SUM-159"){
    runPlugin = "runPlugin('qupath.imagej.detect.cells.WatershedCellDetection', '{\"detectionImageBrightfield\": \"Hematoxylin OD\",  \"requestedPixelSizeMicrons\": 0.5,  \"backgroundRadiusMicrons\": 8.0,  \"medianRadiusMicrons\": 0.0,  \"sigmaMicrons\": 1.5,  \"minAreaMicrons\": 90.0,  \"maxAreaMicrons\": 1200.0,  \"threshold\": 0.1,  \"maxBackground\": 2.0,  \"watershedPostProcess\": false,  \"cellExpansionMicrons\": 5.0,  \"includeNuclei\": false,  \"smoothBoundaries\": true,  \"makeMeasurements\": true}');"
  }else if (cellLine=="SNU-16"){
    runPlugin = "runPlugin('qupath.imagej.detect.cells.WatershedCellDetection', '{\"detectionImageBrightfield\": \"Hematoxylin OD\",  \"requestedPixelSizeMicrons\": 0.5,  \"backgroundRadiusMicrons\": 8.0,  \"medianRadiusMicrons\": 0.0,  \"sigmaMicrons\": 3.5,  \"minAreaMicrons\": 40.0,  \"maxAreaMicrons\": 800.0,  \"threshold\": 0.1,  \"maxBackground\": 2.0,  \"watershedPostProcess\": false,  \"cellExpansionMicrons\": 5.0,  \"includeNuclei\": false,  \"smoothBoundaries\": true,  \"makeMeasurements\": true}');"
  }else  if(cellLine=="NUGC-4"){
    runPlugin = "runPlugin('qupath.imagej.detect.cells.WatershedCellDetection', '{\"detectionImageBrightfield\": \"Hematoxylin OD\",  \"requestedPixelSizeMicrons\": 0.922,  \"backgroundRadiusMicrons\": 8.0,  \"medianRadiusMicrons\": 2.0,  \"sigmaMicrons\": 1.5,  \"minAreaMicrons\": 10.0,  \"maxAreaMicrons\": 200.0,  \"threshold\": 0.1,  \"maxBackground\": 2.9,  \"watershedPostProcess\": true,  \"cellExpansionMicrons\": 2.5,  \"includeNuclei\": false,  \"smoothBoundaries\": true,  \"makeMeasurements\": true}');"
  }else if(cellLine=="KATOIII"){
    runPlugin = "runPlugin('qupath.imagej.detect.cells.WatershedCellDetection', '{\"detectionImageBrightfield\": \"Hematoxylin OD\",  \"requestedPixelSizeMicrons\": 0.5,  \"backgroundRadiusMicrons\": 8.0,  \"medianRadiusMicrons\": 0.0,  \"sigmaMicrons\": 1.5,  \"minAreaMicrons\": 40.0,  \"maxAreaMicrons\": 1200.0,  \"threshold\": 0.09,  \"maxBackground\": 3.0,  \"watershedPostProcess\": true,  \"cellExpansionMicrons\": 5.0,  \"includeNuclei\": false,  \"smoothBoundaries\": true,  \"makeMeasurements\": true}');"
  }
  qpdir = normalizePath(qpdir)
  paste(" import static qupath.lib.gui.scripting.QPEx.*"
        ," import qupath.lib.gui.tools.MeasurementExporter"
        ," import qupath.lib.objects.PathCellObject"
        ," import java.awt.Color"
        ," import java.awt.*"
        ," import qupath.lib.objects.PathDetectionObject"
        ," import qupath.lib.gui.viewer.OverlayOptions"
        ," import qupath.lib.gui.viewer.overlays.HierarchyOverlay"
        ," import qupath.lib.gui.images.servers.RenderedImageServer"
        ," "
        ," import qupath.lib.gui.viewer.overlays.BufferedImageOverlay"
        ," import qupath.opencv.tools.OpenCVTools"
        ," "
        ," "
        ," //  User enter these information for every project."
        ," //*************************************************"
        ," def PixelWidth_new = 1.000;"
        ," def PixelHeight_new = 1.000;"
        ," def x_left = 100;"
        ," def y_left = 100;"
        ," def w_ROI =  1900;"
        ," def h_ROI = 1100;"
        ," //*************************************************"
        ," "
        ," "
        ," def project = getProject();"
        ," def entry = getProjectEntry();"
        ," def imageData = entry.readImageData();"
        ," def CurrentImageData = getCurrentImageData();"
        ," def hierarchy = imageData.getHierarchy();"
        ," def annotations = hierarchy.getAnnotationObjects();"
        ," "
        ," def server = CurrentImageData.getServer();"
        ," def path = server.getPath();"
        ," def cal = server.getPixelCalibration();"
        ," double pixelWidth = cal.getPixelWidthMicrons();"
        ," double pixelHeight = cal.getPixelHeightMicrons();"
        ," "
        ," "
        ," def filename = entry.getImageName() + '.csv'"
        , "// @TODO: read this info directly from .tif metadata"
        ," if (filename.contains('_20x_')){"
        ,"   print('20x data');"
        ,"   PixelWidth_new = 200/433.77;"
        ,"   print(PixelWidth_new);"
        ,"   setPixelSizeMicrons(PixelWidth_new, PixelWidth_new);"
        ," }else if (filename.contains('_10x_')){"
        ,"   print('10x data');"
        ,"   PixelWidth_new = 400/433.77;"
        ,"   print(PixelWidth_new);"
        ,"   setPixelSizeMicrons(PixelWidth_new, PixelWidth_new);"
        ," }else if (filename.contains('_40x_')){"
        ,"   print('40x data');"
        ,"   PixelWidth_new = 100/433.77;"
        ,"   print(PixelWidth_new);"
        ,"   setPixelSizeMicrons(PixelWidth_new, PixelWidth_new);"
        ," }"
        ," "
        ," setImageType('BRIGHTFIELD_H_E');"
        ," setColorDeconvolutionStains('{\"Name\" : \"H&E default\", \"Stain 1\" : \"Hematoxylin\", \"Values 1\" : \"0.65111 0.70119 0.29049 \", \"Stain 2\" : \"Eosin\", \"Values 2\" : \"0.2159 0.8012 0.5581 \", \"Background\" : \" 255 255 255 \"}');"
        ," def plane = ImagePlane.getPlane(0,0);"
        ," def rectangle = ROIs.createRectangleROI(x_left,y_left,w_ROI,h_ROI,plane);"
        ," def rectangleAnnotation = PathObjects.createAnnotationObject(rectangle);"
        ," QPEx.addObjects(rectangleAnnotation);"
        ," println \"Success\";"
        ," //createSelectAllObject(true);"
        ," selectAnnotations();"
        , runPlugin
        ," "
        ," selectDetections()"
        , paste0("def pathDetection = buildFilePath('",qpdir,"/pred');")
        , paste0("def pathAnnotation = buildFilePath('",qpdir,"/cellpose_count')")
        ," mkdirs(pathDetection);"
        ," mkdirs(pathAnnotation);"
        ," def (basename,ext) = filename.tokenize('.');"
        ," pathDetection = buildFilePath(pathDetection, basename+'.csv');"
        ," pathAnnotation = buildFilePath(pathAnnotation, basename+'.csv');"
        ," saveDetectionMeasurements(pathDetection);"
        ," saveAnnotationMeasurements(pathAnnotation)"
        ," "
        ," // Saving Image to file "
        , paste0("def vis_path = buildFilePath('",qpdir,"/vis');")
        ," mkdirs(vis_path);"
        ," vis_path_instance = buildFilePath(vis_path,basename+'_overlay.tif');"
        ," "
        ," //*********************** Save Labeled Image ****************************"
        ," def downsample = 1"
        ," def viewer = getCurrentViewer()"
        ," def labelServer_rendered = new RenderedImageServer.Builder(CurrentImageData)"
        ,"    .downsamples(downsample)"
        ,"    .layers(new HierarchyOverlay(null, new OverlayOptions(), imageData))"
        ,"    .build()"
        ," writeImage(labelServer_rendered, vis_path_instance)"
        , " "
        ," //------------------------- Save Masks   ----------------------------------"
        , "def SaveBinaryMasks4(server,downsample,basename,Path2SaveResults){" 
        ," int w = (server.getWidth() / downsample) as int" 
        ," int h = (server.getHeight() / downsample) as int" 
        ," def img = new BufferedImage(w, h, BufferedImage.TYPE_BYTE_GRAY)" 
        ," def g2d = img.createGraphics()" 
        ," g2d.scale(1.0/downsample, 1.0/downsample)" 
        ," g2d.setColor(Color.WHITE)" 
        ," for (detection in getDetectionObjects()) {" 
        ,"  roi = detection.getROI()" 
        ,"  def shape = roi.getShape()" 
        ,"  g2d.setPaint(Color.white);" 
        ,"  g2d.fill(shape)" 
        ,"  g2d.setStroke(new BasicStroke(4)); // 8-pixel wide pen" 
        ,"  g2d.setPaint(Color.black);" 
        ,"  g2d.draw(shape)" 
        ," }"   
        ,"   g2d.dispose()"
        ," "  
        ,"   // Write the image" 
        ," def masks_path = buildFilePath(Path2SaveResults,'masks');" 
        ," mkdirs(masks_path);" 
        ," mask_path_instance = buildFilePath(masks_path,basename+'.tif');" 
        ," writeImage(img, mask_path_instance)" 
        ,"}"
        , paste0("   SaveBinaryMasks4(server,downsample,basename,'",qpdir,"');") 
        ," "
        ," //*********************** Save Labeled Image Updated Beacuse of the error Noemi Encountered with viewer *********"
        ," //def downsample = 1"
        ," //def labelServer = new LabeledImageServer.Builder(CurrentImageData)"
        ," //  .backgroundLabel(0, ColorTools.BLACK) // Specify background label (usually 0 or 255)"
        ," //  .downsample(downsample)    // Choose server resolution; this should match the resolution at which tiles are exported"
        ," //  .useCells()"
        ," //  .useInstanceLabels()"
        ," //  .setBoundaryLabel('Ignore', 1) "
        ," //  .multichannelOutput(false) // If true, each label refers to the channel of a multichannel binary image (required for multiclass probability)"
        ," //  .build()", sep="\n" )
}



.SaveProject <- function(QUPATH_PRJ, imgFiles){
  prj = paste("{",
              "  \"version\": \"0.2.3\",",
              "  \"createTimestamp\": 1606857053400,",
              "  \"modifyTimestamp\": 1606857053400,",
              paste0("  \"uri\": \"file:", normalizePath(QUPATH_PRJ), "\","),
              paste0("  \"lastID\": ", length(imgFiles), ","),
              "  \"images\": [", sep="\n" )
  for(i in 1:length(imgFiles)){
    imgFile = normalizePath(imgFiles[i])
    prj = paste(prj,
                "    {",
                "      \"serverBuilder\": {",
                "        \"builderType\": \"uri\",",
                "        \"providerClassName\": \"qupath.lib.images.servers.bioformats.BioFormatsServerBuilder\",",
                paste0("        \"uri\": \"file:",imgFile,"\","),
                "        \"args\": [",
                "          \"--series\",",
                "          \"0\"",
                "        ],",
                "        \"metadata\": {",
                paste0("          \"name\": \"",fileparts(imgFile)$name,fileparts(imgFile)$ext,"\","),
                "          \"width\": 2048,",
                "          \"height\": 1536,",
                "          \"sizeZ\": 1,",
                "          \"sizeT\": 1,",
                "          \"channelType\": \"DEFAULT\",",
                "          \"isRGB\": true,",
                "          \"pixelType\": \"UINT8\",",
                "          \"levels\": [",
                "            {",
                "              \"downsample\": 1.0,",
                "              \"width\": 2048,",
                "              \"height\": 1536",
                "            }",
                "          ],",
                "          \"channels\": [",
                "            {",
                "              \"name\": \"Red\",",
                "              \"color\": -65536",
                "            },",
                "            {",
                "              \"name\": \"Green\",",
                "              \"color\": -16711936",
                "            },",
                "            {",
                "              \"name\": \"Blue\",",
                "              \"color\": -16776961",
                "            }",
                "          ],",
                "          \"pixelCalibration\": {",
                "            \"pixelWidth\": {",
                "              \"value\": 1.0,",
                "              \"unit\": \"µm\"",
                "            },",
                "            \"pixelHeight\": {",
                "              \"value\": 1.0,",
                "              \"unit\": \"µm\"",
                "            },",
                "            \"zSpacing\": {",
                "              \"value\": 1.0,",
                "              \"unit\": \"z-slice\"",
                "            },",
                "            \"timeUnit\": \"SECONDS\",",
                "            \"timepoints\": []",
                "          },",
                "          \"preferredTileWidth\": 2048,",
                "          \"preferredTileHeight\": 170",
                "        }",
                "      },",
                paste0("      \"entryID\": ",i,","),
                paste0("      \"randomizedName\": \"d",i,"f15668-2b0e-",i,"f",i,"e-b953-9794d9047a",i,"b\","),
                paste0("      \"imageName\": \"",fileparts(imgFile)$name,fileparts(imgFile)$ext,"\","),
                "      \"metadata\": {}",
                "    },", sep="\n" );
  }
  prj = gsub(",$","",prj)
  prj = paste(prj,
              "  ]",
              "}", sep="\n" )
  return(prj)
}


GenomePerspectiveView_Bulk<-function(id){
  out=findAllDescendandsOf(id, recursive = T)
  mydb = cloneid::connect2DB()
  stmt = paste0("select distinct origin from Perspective where whichPerspective='GenomePerspective' and origin IN ('",paste(unique(out$id), collapse="','"),"')")
  rs = suppressWarnings(dbSendQuery(mydb, stmt))
  origin=fetch(rs, n=-1)[,"origin"]
  
  p=sapply(origin, function(x) getSubProfiles(cloneID_or_sampleName = x, whichP = "GenomePerspective"))
  p=do.call(cbind, p)
  gplots::heatmap.2(t(p), trace = "n")
}


# Helper function to retrieve ancestors
.get_ancestors <- function(conn, start_id, cellLine) {
  ancestors <- c()
  current_id <- start_id
  while (!is.na(current_id) && !is.null(current_id)) {
    ancestors <- c(ancestors, current_id)
    query <- paste0("SELECT passaged_from_id1 FROM Passaging WHERE cellLine='",cellLine,"' and id = '", current_id,"'")
    current_id <- dbGetQuery(conn, query)$passaged_from_id1
  }
  
  return(ancestors)
}

find_mrca <- function(conn, id1, id2, cellLine) {
  # Get ancestors for both ids
  ancestors1 <- .get_ancestors(conn, id1,cellLine)
  ancestors2 <- .get_ancestors(conn, id2,cellLine)
  
  # Find the most recent common ancestor
  common_ancestors <- intersect(ancestors1, ancestors2)
  
  if (length(common_ancestors) > 0) {
    return(common_ancestors[1])  # The most recent common ancestor
  } else {
    return(NA)  # No common ancestor found
  }
}


# Function to group IDs based on pedigree closeness
pedigree_dist <- function(conn, ids, cellLine) {
  n <- length(ids)
  
  if (n < 3) {
    stop("Need at least 3 IDs to form groups.")
  }
  
  # Compute MRCA distances
  distance_matrix <- matrix(Inf, n, n, dimnames = list(ids, ids))
  
  for (i in 1:(n-1)) {
    for (j in (i+1):n) {
      mrca <- find_mrca(conn, ids[i], ids[j], cellLine)
      if (!is.na(mrca)) {
        # Define distance as number of generations from MRCA to current ID
        depth_i <- which(.get_ancestors(conn, ids[i], cellLine) == mrca)
        depth_j <- which(.get_ancestors(conn, ids[j], cellLine) == mrca)
        distance_matrix[i, j] <- depth_i + depth_j
        distance_matrix[j, i] <- distance_matrix[i, j]
      }
    }
  }
  
  # # Convert to hierarchical clustering
  # dist_obj <- as.dist(distance_matrix)
  # hc <- hclust(dist_obj, method = "complete")
  # 
  # # Determine number of clusters
  # num_groups <- ceiling(n / 3.5)  # Approximate groups of 3-4
  # clusters <- cutree(hc, k = num_groups)
  # 
  # # Format output as tuples
  # grouped_ids <- split(ids, clusters)
  
  return(distance_matrix)
}


# ---------------------------------------------------------------------------
# Subtree SQL export — internal helpers and public function
# ---------------------------------------------------------------------------

#' Serialize a single scalar DB value to a SQL literal.
#'
#' @param x     A scalar value as returned by RMySQL fetch().
#' @param conn  An open DBI/RMySQL connection (used for dbQuoteString).
#' @param blob  Logical. If TRUE, treat a non-NA character value as raw binary
#'              and emit a MySQL hex literal (0x...). Use for mediumblob columns
#'              such as Perspective.profile and Loci.content.
#' @return A character string containing the SQL literal, ready for INSERT.
.sql_val <- function(x, conn, blob = FALSE) {
  # NULL / empty
  if (is.null(x) || length(x) == 0) return("NULL")

  # raw vector -> hex literal.
  # Must be checked BEFORE the length > 1 guard: a raw vector of N bytes
  # represents one blob value, not N scalars.
  if (is.raw(x)) {
    if (length(x) == 0) return("NULL")
    return(paste0("0x", paste(sprintf("%02X", as.integer(x)), collapse = "")))
  }

  # list column (RMySQL mediumblob return type): each cell is a raw vector
  # or NULL.  Handle both before the scalar length guard.
  if (is.list(x) && length(x) == 1) {
    inner <- x[[1]]
    if (is.null(inner)) return("NULL")
    if (is.raw(inner)) {
      if (length(inner) == 0) return("NULL")
      return(paste0("0x", paste(sprintf("%02X", as.integer(inner)), collapse = "")))
    }
  }

  # All remaining types are expected to be scalar.
  if (length(x) > 1) stop(".sql_val expects a scalar, got length ", length(x))
  if (is.na(x))      return("NULL")

  # logical -> 0/1
  if (is.logical(x)) return(as.character(as.integer(x)))

  # POSIXct / POSIXlt timestamps -> 'YYYY-MM-DD HH:MM:SS'
  if (inherits(x, c("POSIXct", "POSIXlt", "POSIXt"))) {
    return(as.character(DBI::dbQuoteString(conn, format(x, "%Y-%m-%d %H:%M:%S"))))
  }

  # Date -> 'YYYY-MM-DD'
  if (inherits(x, "Date")) {
    return(as.character(DBI::dbQuoteString(conn, format(x, "%Y-%m-%d"))))
  }

  # numeric -> decimal notation, no scientific notation.
  # digits=15 preserves full double precision (matches MySQL DOUBLE).
  if (is.numeric(x)) return(format(x, scientific = FALSE, trim = TRUE, digits = 15))

  # character / factor marked as blob column -> hex literal
  if (blob) {
    raw_bytes <- charToRaw(as.character(x))
    if (length(raw_bytes) == 0) return("NULL")
    return(paste0("0x", paste(sprintf("%02X", as.integer(raw_bytes)), collapse = "")))
  }

  # character / factor -> quoted string via DBI (handles escaping).
  # as.character() coerces the SQL class returned by dbQuoteString to plain
  # character so callers always receive a uniform character vector.
  return(as.character(DBI::dbQuoteString(conn, as.character(x))))
}


#' Compute the set of Passaging.id values that form the export subtree.
#'
#' Descendants are traced exclusively through passaged_from_id1, matching the
#' convention used by findAllDescendandsOf() and getPedigreeTree() throughout
#' this package. passaged_from_id2 is NOT used for traversal in v1.
#'
#' Uses .db_fetch() for all reads — consistent with the package's established
#' read helper. No writes are performed.
#'
#' @param root_id   Character. The Passaging.id at the root of the subtree.
#' @param recursive Logical. FALSE (default) = root + direct children only
#'                  (rows where passaged_from_id1 == root_id). TRUE = full
#'                  recursive closure via passaged_from_id1, matching the
#'                  findAllDescendandsOf() traversal pattern.
#' @param conn      Open DBI/RMySQL connection. Read-only — no writes are made.
#' @return Character vector of Passaging.id values in the subtree, with
#'         root_id first and remaining ids in DFS order. Guaranteed unique.
.subtree_ids <- function(root_id, recursive = FALSE, conn) {
  # Fail fast if root does not exist — avoids silently emitting an empty dump.
  # root_id is quoted via DBI::dbQuoteString; not interpolated raw.
  root_check <- .db_fetch(
    paste0("SELECT id FROM Passaging WHERE id = ",
           DBI::dbQuoteString(conn, root_id),
           " ORDER BY id"),
    conn = conn
  )
  if (nrow(root_check) == 0) {
    stop("Passaging.id not found: ", root_id)
  }

  # Internal helper: collect descendant ids via passaged_from_id1.
  # For recursive=FALSE this is called once; the guard prevents further descent.
  # ORDER BY id makes child enumeration deterministic regardless of DB storage order.
  .trace_children <- function(parent_id) {
    kids <- .db_fetch(
      paste0("SELECT id FROM Passaging WHERE passaged_from_id1 = ",
             DBI::dbQuoteString(conn, parent_id),
             " ORDER BY id"),
      conn = conn
    )
    ids <- kids$id  # character vector, length 0 if no children
    if (recursive && length(ids) > 0) {
      for (kid_id in ids) {
        ids <- c(ids, .trace_children(kid_id))
      }
    }
    return(ids)
  }

  descendant_ids <- .trace_children(root_id)
  return(unique(c(root_id, descendant_ids)))
}


#' Serialize a data frame of DB rows into a vector of SQL INSERT statements.
#'
#' Pure text serialization — no DB writes of any kind. The caller is
#' responsible for fetching and ordering rows before passing them here.
#' Row order and column order are preserved exactly as supplied; no sorting
#' or reordering is performed inside this function. conn is used solely for
#' DBI::dbQuoteString() (string escaping) — no queries are sent.
#'
#' RMySQL returns mediumblob columns as list columns (each cell is a raw vector
#' or NULL). This function detects list columns and extracts values with [[]]
#' rather than [] to recover the actual raw vector before passing to .sql_val().
#'
#' @param table     Character. Table name (will be backtick-quoted).
#' @param df        Data frame of rows to INSERT, as returned by fetch().
#' @param conn      Open DBI/RMySQL connection (passed to .sql_val for quoting).
#' @param blob_cols Character vector of column names that are blob/binary type.
#'                  Values in these columns are serialized as 0x<HEX> literals.
#'                  Known v1 blob columns: "profile" (Perspective), "content" (Loci).
#' @return Character vector of INSERT statements, one per row. Empty vector if
#'         df has zero rows.
.rows_to_inserts <- function(table, df, conn, blob_cols = character(0)) {
  if (nrow(df) == 0) return(character(0))

  cols     <- names(df)
  col_list <- paste(paste0("`", cols, "`"), collapse = ", ")
  tbl      <- paste0("`", table, "`")

  # Pre-compute which columns are list columns (RMySQL blob return type)
  is_list_col <- vapply(cols, function(col) is.list(df[[col]]), logical(1))

  stmts <- vapply(seq_len(nrow(df)), function(i) {
    vals <- vapply(seq_along(cols), function(j) {
      col <- cols[j]
      # Use [[i]] for list columns (blobs), [i] for atomic columns
      x <- if (is_list_col[j]) df[[col]][[i]] else df[[col]][i]
      .sql_val(x, conn, blob = col %in% blob_cols)
    }, character(1))
    paste0("INSERT INTO ", tbl, " (", col_list, ") VALUES (",
           paste(vals, collapse = ", "), ");")
  }, character(1))

  return(stmts)
}


#' Fetch all FK-required dependency rows for an already-fetched set of Passaging rows.
#'
#' Covers the four tables that Passaging directly references:
#'   CellLinesAndPatients  (via Passaging.cellLine)
#'   Flask                 (via Passaging.flask)
#'   Media                 (via Passaging.media)
#'   MediaIngredients      (via the 13 FK columns in Media; schema lines 301-314)
#'
#' Each table is queried only for the referenced key values present in the
#' supplied passaging_rows; NULLs are skipped. Rows are ordered by primary key
#' for deterministic output. Missing referenced rows are treated as a DB
#' integrity error and cause stop().
#'
#' @param passaging_rows Data frame of Passaging rows (already fetched).
#' @param conn           Open DBI/RMySQL connection. Read-only.
#' @return Named list with elements: cell_lines, flasks, media, media_ingredients.
#'         Each element is a data frame (zero rows possible; columns always present).
.fetch_dependency_closure <- function(passaging_rows, conn) {

  # Build a safe SQL IN clause from a character vector of already-quoted literals.
  # Values are quoted via DBI::dbQuoteString before being passed here.
  .in_clause_chr <- function(vals) {
    paste0("(", paste(DBI::dbQuoteString(conn, vals), collapse = ", "), ")")
  }

  # Build a safe SQL IN clause for integer FK values (flask, media ids).
  .in_clause_int <- function(vals) {
    paste0("(", paste(as.integer(vals), collapse = ", "), ")")
  }

  # Return an empty data frame with the correct column schema for a table.
  .empty_table <- function(table_name) {
    .db_fetch(paste0("SELECT * FROM `", table_name, "` WHERE 1=0"), conn = conn)
  }

  # All FK column names in Media that reference MediaIngredients(name).
  # Derived from CLONEID_schema.sql lines 301-314 (Media_ibfk_1 through _14).
  MEDIA_INGREDIENT_FK_COLS <- c(
    "base1", "base2", "FBS", "EnergySource2", "EnergySource",
    "HEPES", "Salt", "antibiotic", "antibiotic2", "antimycotic",
    "Stressor", "antibiotic3", "antibiotic4"
  )

  # -------------------------------------------------------------------------
  # CellLinesAndPatients — referenced by Passaging.cellLine (varchar FK)
  # -------------------------------------------------------------------------
  cl_vals <- unique(passaging_rows$cellLine[!is.na(passaging_rows$cellLine)])
  if (length(cl_vals) > 0) {
    cell_lines <- .db_fetch(
      paste0("SELECT * FROM `CellLinesAndPatients` WHERE `name` IN ",
             .in_clause_chr(cl_vals), " ORDER BY `name`"),
      conn = conn
    )
    # Case-insensitive comparison: MySQL's default collation resolves varchar
    # lookups case-insensitively, so normalise both sides with tolower() before
    # setdiff to avoid false "missing" reports when capitalisation differs.
    # The fetched rows (cell_lines) are returned unchanged.
    missing <- setdiff(tolower(cl_vals), tolower(cell_lines$name))
    if (length(missing) > 0) {
      stop("CellLinesAndPatients rows missing for cellLine value(s): ",
           paste(missing, collapse = ", "))
    }
  } else {
    cell_lines <- .empty_table("CellLinesAndPatients")
  }

  # -------------------------------------------------------------------------
  # Flask — referenced by Passaging.flask (int FK)
  # -------------------------------------------------------------------------
  flask_vals <- unique(passaging_rows$flask[!is.na(passaging_rows$flask)])
  if (length(flask_vals) > 0) {
    flasks <- .db_fetch(
      paste0("SELECT * FROM `Flask` WHERE `id` IN ",
             .in_clause_int(flask_vals), " ORDER BY `id`"),
      conn = conn
    )
    missing <- setdiff(as.integer(flask_vals), as.integer(flasks$id))
    if (length(missing) > 0) {
      stop("Flask rows missing for flask id(s): ",
           paste(missing, collapse = ", "))
    }
  } else {
    flasks <- .empty_table("Flask")
  }

  # -------------------------------------------------------------------------
  # Media — referenced by Passaging.media (int FK)
  # -------------------------------------------------------------------------
  media_vals <- unique(passaging_rows$media[!is.na(passaging_rows$media)])
  if (length(media_vals) > 0) {
    media_rows <- .db_fetch(
      paste0("SELECT * FROM `Media` WHERE `id` IN ",
             .in_clause_int(media_vals), " ORDER BY `id`"),
      conn = conn
    )
    missing <- setdiff(as.integer(media_vals), as.integer(media_rows$id))
    if (length(missing) > 0) {
      stop("Media rows missing for media id(s): ",
           paste(missing, collapse = ", "))
    }
  } else {
    media_rows <- .empty_table("Media")
  }

  # -------------------------------------------------------------------------
  # MediaIngredients — referenced by 13 FK columns in Media
  # Collect all non-null ingredient names across all returned Media rows.
  # -------------------------------------------------------------------------
  mi_vals <- character(0)
  if (nrow(media_rows) > 0) {
    present_fk_cols <- intersect(MEDIA_INGREDIENT_FK_COLS, names(media_rows))
    for (col in present_fk_cols) {
      mi_vals <- c(mi_vals, media_rows[[col]][!is.na(media_rows[[col]])])
    }
    mi_vals <- unique(mi_vals)
  }
  if (length(mi_vals) > 0) {
    media_ingredients <- .db_fetch(
      paste0("SELECT * FROM `MediaIngredients` WHERE `name` IN ",
             .in_clause_chr(mi_vals), " ORDER BY `name`"),
      conn = conn
    )
    # Case-insensitive comparison for the same reason as CellLinesAndPatients.
    missing <- setdiff(tolower(mi_vals), tolower(media_ingredients$name))
    if (length(missing) > 0) {
      stop("MediaIngredients rows missing for ingredient name(s): ",
           paste(missing, collapse = ", "))
    }
  } else {
    media_ingredients <- .empty_table("MediaIngredients")
  }

  list(
    cell_lines        = cell_lines,
    flasks            = flasks,
    media             = media_rows,
    media_ingredients = media_ingredients
  )
}


#' Fetch Perspective rows rooted in the exported passaging id set, and the
#' Loci rows they reference.
#'
#' Implements the two-stage perspective closure described in the report:
#'   1. SELECT all Perspective rows whose origin is in export_ids.
#'   2. Derive the referenced Loci.id set from those rows (via profile_loci).
#'   3. Fetch those Loci rows.
#'
#' This matches the pattern already used in GenomePerspectiveView_Bulk()
#' (InVitroUtils.R:1210-1220), which queries Perspective by origin IN (ids)
#' after obtaining descendant ids from findAllDescendandsOf().
#'
#' Identity, FlowCytometry, IdentitySub, and PerspectivePartial are explicitly
#' excluded from v1 scope.
#'
#' Both Perspective.profile and Loci.content are mediumblob. The caller must
#' pass blob_cols = "profile" when serializing Perspective rows, and
#' blob_cols = "content" when serializing Loci rows, via .rows_to_inserts().
#'
#' @param export_ids Character vector of Passaging.id values in the subtree.
#' @param conn       Open DBI/RMySQL connection. Read-only.
#' @return Named list with elements:
#'   perspectives — data frame of Perspective rows ordered by cloneID (PK).
#'   loci         — data frame of Loci rows ordered by id (PK).
#'                  Empty data frames (with correct columns) when no rows exist.
.fetch_perspective_closure <- function(export_ids, conn) {

  # Return an empty data frame with the correct column schema for a table.
  .empty_table <- function(table_name) {
    .db_fetch(paste0("SELECT * FROM `", table_name, "` WHERE 1=0"), conn = conn)
  }

  # -------------------------------------------------------------------------
  # Stage 1: Perspective rows whose origin is in the exported passaging id set.
  # Ordered by cloneID (integer primary key) for deterministic row order.
  # -------------------------------------------------------------------------
  if (length(export_ids) > 0) {
    in_clause <- paste0(
      "(",
      paste(DBI::dbQuoteString(conn, export_ids), collapse = ", "),
      ")"
    )
    perspective_rows <- .db_fetch(
      paste0("SELECT * FROM `Perspective` WHERE `origin` IN ",
             in_clause, " ORDER BY `cloneID`"),
      conn = conn
    )
  } else {
    perspective_rows <- .empty_table("Perspective")
  }

  # -------------------------------------------------------------------------
  # Stage 2: Loci rows referenced by the included Perspective rows.
  # profile_loci is nullable — NULLs are skipped.
  # Ordered by id (integer primary key) for deterministic row order.
  # -------------------------------------------------------------------------
  loci_ids <- unique(
    perspective_rows$profile_loci[!is.na(perspective_rows$profile_loci)]
  )

  if (length(loci_ids) > 0) {
    loci_rows <- .db_fetch(
      paste0("SELECT * FROM `Loci` WHERE `id` IN (",
             paste(as.integer(loci_ids), collapse = ", "),
             ") ORDER BY `id`"),
      conn = conn
    )
    missing <- setdiff(as.integer(loci_ids), as.integer(loci_rows$id))
    if (length(missing) > 0) {
      stop("Loci rows missing for profile_loci id(s): ",
           paste(missing, collapse = ", "))
    }
  } else {
    loci_rows <- .empty_table("Loci")
  }

  list(
    perspectives = perspective_rows,
    loci         = loci_rows
  )
}


#' Fetch storage/location rows tied to the exported Passaging id set.
#'
#' Covers the two storage tables that reference Passaging(id):
#'   LiquidNitrogen  FK: id -> Passaging.id (nullable; composite PK)
#'   Minus80Freezer  FK: id -> Passaging.id (nullable; composite PK)
#'
#' Only rows where id IN export_ids are returned. Storage slots that happen to
#' be unoccupied or belong to other passaging ids are excluded automatically by
#' the IN clause. No integrity check is applied — it is normal for exported
#' Passaging ids to have no storage rows.
#'
#' Rows are ordered by each table's primary key for deterministic output:
#'   LiquidNitrogen:  (Rack, Row, BoxRow, BoxColumn)
#'   Minus80Freezer:  (Drawer, Position, BoxRow, BoxColumn)
#'
#' @param export_ids Character vector of Passaging.id values in the subtree.
#' @param conn       Open DBI/RMySQL connection. Read-only.
#' @return Named list: liquid_nitrogen, minus80_freezer.
#'         Each is a data frame (zero rows when nothing found; columns always present).
.fetch_storage_rows <- function(export_ids, conn) {

  # Return an empty data frame with the correct column schema for a table.
  .empty_table <- function(table_name) {
    .db_fetch(paste0("SELECT * FROM `", table_name, "` WHERE 1=0"), conn = conn)
  }

  if (length(export_ids) == 0) {
    return(list(
      liquid_nitrogen = .empty_table("LiquidNitrogen"),
      minus80_freezer = .empty_table("Minus80Freezer")
    ))
  }

  in_clause <- paste0(
    "(",
    paste(DBI::dbQuoteString(conn, export_ids), collapse = ", "),
    ")"
  )

  # LiquidNitrogen — PK: (Rack, Row, BoxRow, BoxColumn)
  liquid_nitrogen <- .db_fetch(
    paste0("SELECT * FROM `LiquidNitrogen` WHERE `id` IN ",
           in_clause,
           " ORDER BY `Rack`, `Row`, `BoxRow`, `BoxColumn`"),
    conn = conn
  )

  # Minus80Freezer — PK: (Drawer, Position, BoxRow, BoxColumn)
  minus80_freezer <- .db_fetch(
    paste0("SELECT * FROM `Minus80Freezer` WHERE `id` IN ",
           in_clause,
           " ORDER BY `Drawer`, `Position`, `BoxRow`, `BoxColumn`"),
    conn = conn
  )

  list(
    liquid_nitrogen = liquid_nitrogen,
    minus80_freezer = minus80_freezer
  )
}


#' Decode molecular profiles for exported Perspective rows.
#'
#' For each unique (whichPerspective, origin) pair in \code{perspectives_df},
#' identifies every internal node in the exported clone tree (rows whose
#' \code{cloneID} appears as the \code{parent} of at least one other row),
#' then calls \code{getSubProfiles()} for each internal node and column-binds
#' the results into a single matrix covering all non-root clones in the tree.
#'
#' The decoded matrices are accumulated in a two-level named list:
#' \preformatted{
#'   profiles$<whichPerspective>$<origin> = numeric matrix
#'     rows    = genomic loci (chr:start-end strings)
#'     columns = clone identifiers (all non-root clones in the exported tree)
#'     values  = modality-specific numeric values (e.g. copy number, expression)
#' }
#'
#' @param perspectives_df  Data frame of Perspective rows as returned by
#'   \code{.fetch_perspective_closure()$perspectives}.  Must include a
#'   \code{parent} column (present in all exports produced by this package).
#'   May be zero-row.
#'
#' @return Named list with two elements:
#' \describe{
#'   \item{profiles}{Two-level named list as described above.  Empty list
#'     (\code{list()}) when there are no perspective rows or all pairs fail.}
#'   \item{warnings}{Character vector recording every skipped
#'     (whichPerspective, origin) pair and its reason.  \code{character(0)}
#'     when all decodes succeed.}
#' }
#'
#' @section Full-tree traversal (v2):
#' \code{getSubProfiles(cloneID)} returns only the direct children of the
#' specified clone (one level deep).  To decode the entire nested clone tree,
#' this function identifies all internal nodes from the exported Perspective
#' rows and calls \code{getSubProfiles} once per internal node.  The per-node
#' results are column-bound; duplicate columns (if any) are removed.
#' Partial failure on a single node is recorded in \code{warnings} but does
#' not abort decoding of the remaining nodes or pairs.
#'
#' @section Connection note:
#' This helper calls \code{getSubProfiles()}, which dispatches to Java's
#' \code{cloneid.Manager} via rJava (\code{.jcall}).  It does \strong{not}
#' accept or use an RMySQL connection object.  Decoding therefore depends on
#' the active Java/CLONEID runtime environment being initialised (package
#' \code{.onLoad}), not on any R-level \code{conn} argument.
.decode_perspective_profiles <- function(perspectives_df) {
  warn    <- character(0)
  profiles <- list()

  # Nothing to decode.
  if (is.null(perspectives_df) || nrow(perspectives_df) == 0L) {
    return(list(profiles = profiles, warnings = warn))
  }

  # Guard: required columns must be present.
  # 'parent' is needed to identify internal nodes; 'size' is no longer required.
  required_cols <- c("whichPerspective", "origin", "cloneID", "parent")
  missing_cols  <- setdiff(required_cols, colnames(perspectives_df))
  if (length(missing_cols) > 0L) {
    warn <- c(warn, paste0(
      "perspectives_df missing required column(s): ",
      paste(missing_cols, collapse = ", "),
      "; no profiles decoded."
    ))
    return(list(profiles = profiles, warnings = warn))
  }

  # Enumerate unique (whichPerspective, origin) pairs.
  pairs <- unique(
    perspectives_df[, c("whichPerspective", "origin"), drop = FALSE]
  )

  for (i in seq_len(nrow(pairs))) {
    persp_type <- pairs$whichPerspective[i]
    origin     <- pairs$origin[i]
    key        <- paste0("[", persp_type, " / origin=", origin, "]")

    sub <- perspectives_df[
      perspectives_df$whichPerspective == persp_type &
        perspectives_df$origin == origin,
      ,
      drop = FALSE
    ]

    # v2: identify all internal nodes — cloneIDs that appear as 'parent' of at
    # least one other row in this (origin, whichPerspective) slice.
    # Calling getSubProfiles() for every internal node and combining the results
    # gives profiles for all non-root clones in the exported tree, not just the
    # root's direct children (which is what a single root-only call would return).
    parent_ids   <- unique(sub$parent[!is.na(sub$parent)])
    internal_ids <- intersect(as.integer(sub$cloneID), as.integer(parent_ids))

    if (length(internal_ids) == 0L) {
      warn <- c(warn, paste0(
        key, " skipped: no internal nodes found in exported Perspective rows ",
        "(all rows may be leaves, or the 'parent' column may be all NA)."
      ))
      next
    }

    # Call getSubProfiles for each internal node to collect its children's
    # profiles.  Errors on individual nodes are captured without aborting the
    # remaining nodes or pairs.
    profile_parts <- list()
    for (cid in internal_ids) {
      mat <- tryCatch(
        getSubProfiles(cloneID_or_sampleName = cid, whichP = persp_type),
        error = function(e) {
          warn <<- c(warn, paste0(
            key, " getSubProfiles(", cid, ") error: ", conditionMessage(e)
          ))
          NULL
        }
      )
      if (!is.null(mat) && !is.null(ncol(mat)) && ncol(mat) > 0L) {
        profile_parts[[length(profile_parts) + 1L]] <- mat
      }
    }

    if (length(profile_parts) == 0L) next

    combined <- do.call(cbind, profile_parts)
    # Remove duplicate columns — same clone can appear as child of multiple
    # nodes in degenerate trees; keep the first occurrence.
    combined <- combined[, !duplicated(colnames(combined)), drop = FALSE]

    if (is.null(profiles[[persp_type]])) profiles[[persp_type]] <- list()
    profiles[[persp_type]][[origin]] <- combined
  }

  list(profiles = profiles, warnings = warn)
}


#' Apply the external parent reference policy to exported Passaging rows.
#'
#' Because Passaging.passaged_from_id1 and passaged_from_id2 are self-referencing
#' FKs, a root that is not a true lineage root will have a passaged_from_id1
#' pointing outside the exported set. Three policies are supported:
#'
#'   "nullify" (default): set any passaged_from_id1 or passaged_from_id2 that
#'     points outside export_ids to NA in the returned data frame, and record
#'     every such detachment in detached_parent_refs. The dump imports cleanly
#'     without expanding scope.
#'
#'   "error": stop() immediately if any exported row references a non-exported
#'     parent. Useful for asserting that the chosen root is a true lineage root.
#'
#'   "include": iteratively fetch the missing parent Passaging rows from the DB
#'     and expand export_ids until full FK closure is reached. Requires conn.
#'     May significantly grow the dump if the ancestry chain is long.
#'
#' @param passaging_rows  Data frame of Passaging rows to export (already fetched).
#' @param export_ids      Character vector of currently-exported Passaging.id values.
#' @param policy          One of "nullify", "include", "error".
#' @param conn            Open DBI/RMySQL connection. Required only for "include".
#' @return Named list:
#'   passaging_rows       — data frame, modified per policy.
#'   export_ids           — character vector, expanded if policy="include".
#'   detached_parent_refs — data frame(passaging_id, field, original_value);
#'                          records every field nullified (empty for "include"/"error").
.apply_external_parent_policy <- function(passaging_rows, export_ids,
                                          policy, conn = NULL) {
  policy <- match.arg(policy, c("nullify", "include", "error"))

  # Empty detachment log — columns match regardless of how many rows are added.
  detached_refs <- data.frame(
    passaging_id   = character(0),
    field          = character(0),
    original_value = character(0),
    stringsAsFactors = FALSE
  )

  # -------------------------------------------------------------------------
  # "error" — fail immediately if any external parent reference exists.
  # -------------------------------------------------------------------------
  if (policy == "error") {
    ext1 <- passaging_rows$passaged_from_id1[
      !is.na(passaging_rows$passaged_from_id1) &
        !(passaging_rows$passaged_from_id1 %in% export_ids)
    ]
    ext2 <- passaging_rows$passaged_from_id2[
      !is.na(passaging_rows$passaged_from_id2) &
        !(passaging_rows$passaged_from_id2 %in% export_ids)
    ]
    ext_all <- unique(c(ext1, ext2))
    if (length(ext_all) > 0) {
      stop("external_parent_policy='error': exported Passaging rows reference ",
           "non-exported parent id(s): ", paste(sort(ext_all), collapse = ", "))
    }
    return(list(passaging_rows       = passaging_rows,
                export_ids           = export_ids,
                detached_parent_refs = detached_refs))
  }

  # -------------------------------------------------------------------------
  # "nullify" — set external parent refs to NA and log each detachment.
  # -------------------------------------------------------------------------
  if (policy == "nullify") {
    for (field in c("passaged_from_id1", "passaged_from_id2")) {
      vals         <- passaging_rows[[field]]
      external_mask <- !is.na(vals) & !(vals %in% export_ids)
      if (any(external_mask)) {
        detached_refs <- rbind(
          detached_refs,
          data.frame(
            passaging_id   = passaging_rows$id[external_mask],
            field          = field,
            original_value = vals[external_mask],
            stringsAsFactors = FALSE
          )
        )
        passaging_rows[[field]][external_mask] <- NA
      }
    }
    return(list(passaging_rows       = passaging_rows,
                export_ids           = export_ids,
                detached_parent_refs = detached_refs))
  }

  # -------------------------------------------------------------------------
  # "include" — iteratively pull in missing parent rows until FK closure.
  # Each iteration fetches newly-discovered external parents, adds them to
  # passaging_rows, and continues until no unresolved external refs remain.
  # -------------------------------------------------------------------------
  if (is.null(conn)) {
    stop(".apply_external_parent_policy with policy='include' requires conn")
  }

  seen_ids <- export_ids

  repeat {
    ext1 <- passaging_rows$passaged_from_id1[
      !is.na(passaging_rows$passaged_from_id1) &
        !(passaging_rows$passaged_from_id1 %in% seen_ids)
    ]
    ext2 <- passaging_rows$passaged_from_id2[
      !is.na(passaging_rows$passaged_from_id2) &
        !(passaging_rows$passaged_from_id2 %in% seen_ids)
    ]
    missing_ids <- unique(c(ext1, ext2))
    if (length(missing_ids) == 0) break

    in_clause <- paste0(
      "(",
      paste(DBI::dbQuoteString(conn, missing_ids), collapse = ", "),
      ")"
    )
    new_rows <- .db_fetch(
      paste0("SELECT * FROM `Passaging` WHERE `id` IN ",
             in_clause, " ORDER BY `id`"),
      conn = conn
    )
    not_found <- setdiff(missing_ids, new_rows$id)
    if (length(not_found) > 0) {
      stop("external_parent_policy='include': referenced parent id(s) not found ",
           "in Passaging: ", paste(sort(not_found), collapse = ", "))
    }
    passaging_rows <- rbind(passaging_rows, new_rows)
    seen_ids       <- c(seen_ids, new_rows$id)
  }

  # Deduplicate and re-sort by id for deterministic order.
  passaging_rows <- passaging_rows[!duplicated(passaging_rows$id), ]
  passaging_rows <- passaging_rows[order(passaging_rows$id), ]

  list(passaging_rows       = passaging_rows,
       export_ids           = seen_ids,
       detached_parent_refs = detached_refs)
}


#' Export a subtree bundle rooted at a single Passaging.id.
#'
#' Produces a self-contained, FK-safe bundle of the in-vitro lineage subtree
#' rooted at \code{id}, including all required dependency rows.  The bundle is
#' returned as an R list and optionally written to disk.
#'
#' Orchestration order (important for external_parent_policy = "include"):
#'   1. Compute subtree ids via passaged_from_id1 traversal.
#'   2. Fetch Passaging rows.
#'   3. Apply external parent policy — export_ids and passaging_rows may expand.
#'   4. Fetch dependency closure (CellLinesAndPatients, Flask, Media,
#'      MediaIngredients) from the final expanded passaging_rows.
#'   5. Fetch Perspective + Loci closure from the final expanded export_ids.
#'   6. Fetch storage rows from the final expanded export_ids.
#'
#' FK-safe INSERT order (relevant for format = "sql"):
#'   CellLinesAndPatients -> MediaIngredients -> Media -> Flask ->
#'   Loci -> Passaging -> Perspective ->
#'   LiquidNitrogen -> Minus80Freezer
#'
#' V1 policy for Perspective.parent and Perspective.rootID:
#'   Both columns are plain int DEFAULT NULL with no FK constraint in the schema
#'   (CLONEID_schema.sql lines 461-487). MySQL will not enforce them on import,
#'   so they cannot cause FK violations. They are exported as-is without
#'   additional closure. Some values may reference Perspective.cloneID rows
#'   outside the exported slice; this is deliberate and safe in v1.
#'
#' @param id                    Character. Passaging.id of the subtree root.
#' @param file                  Character path or NULL. If non-NULL the bundle
#'                              is written to this path: saveRDS() for "rds",
#'                              writeLines() for "sql". Overwrites unconditionally.
#' @param format                One of "rds" (default) or "sql".
#' @param recursive             Logical. FALSE (default) = root + direct children.
#'                              TRUE = full descendant closure.
#' @param include_storage       Logical. Include LiquidNitrogen and Minus80Freezer rows.
#' @param include_perspectives  Logical. Include Perspective and Loci rows.
#' @param decode_profiles_recursive  Logical. FALSE (default) = shallow decode:
#'                              \code{.decode_perspective_profiles()} is called and
#'                              its result stored in \code{bundle$decoded}, using the
#'                              current one-level-per-internal-node traversal.
#'                              TRUE = full recursive decode (not yet implemented;
#'                              will stop with an informative error).
#'                              Decoding depends on the active Java/CLONEID runtime,
#'                              not the RMySQL connection. Partial decode failures
#'                              are recorded in \code{metadata$decode_warnings} and
#'                              do not abort the export. Decoding only runs when
#'                              \code{format="rds"}; it is silently skipped for
#'                              \code{format="sql"}.
#' @param conn                  Open DBI/RMySQL connection, or NULL to open one.
#' @param external_parent_policy One of "nullify" (default), "include", "error".
#' @return Named list with fields present for both formats:
#'   metadata             — list: root_id, format, recursive, include_storage,
#'                          include_perspectives, decode_profiles_recursive,
#'                          external_parent_policy, exported_at, package_version,
#'                          decode_warnings (character vector; empty if none).
#'   export_ids           — character vector of exported Passaging.id values
#'                          (may be expanded by policy="include").
#'   detached_parent_refs — data frame(passaging_id, field, original_value)
#'                          listing every parent ref nullified (policy="nullify").
#'   tables               — named list of data frames (actual rows, not counts),
#'                          in FK-safe order.  DB-faithful; never mutated by decoding.
#'   decoded              — two-level named list profiles$<whichPerspective>$<origin>
#'                          = numeric matrix (format="rds"; shallow decode by default),
#'                          or NULL (format="sql").
#'   sql                  — complete SQL dump string (format="sql") or NULL.
#'   statements           — character vector: "START TRANSACTION;", INSERTs,
#'                          "COMMIT;" (format="sql") or NULL.
export_passaging_subtree_bundle <- function(
    id,
    file                   = NULL,
    format                 = c("rds", "sql"),
    recursive              = FALSE,
    include_storage        = TRUE,
    include_perspectives   = TRUE,
    decode_profiles_recursive = FALSE,
    conn                   = NULL,
    external_parent_policy = c("nullify", "include", "error")) {

  format                 <- match.arg(format)
  external_parent_policy <- match.arg(external_parent_policy)

  decode_profiles_recursive <- isTRUE(decode_profiles_recursive)

  # Connection lifecycle: open a fresh connection if none supplied, and ensure
  # it is closed on exit only if we opened it (caller-supplied connections are
  # left open for the caller to manage).
  opened_conn <- FALSE
  if (is.null(conn)) {
    conn        <- connect2DB()
    opened_conn <- TRUE
  }
  on.exit({
    if (opened_conn) try(dbDisconnect(conn), silent = TRUE)
  }, add = TRUE)

  # -------------------------------------------------------------------------
  # 1. Compute subtree ids (fails fast if root not found)
  # -------------------------------------------------------------------------
  export_ids <- .subtree_ids(id, recursive = recursive, conn = conn)

  # -------------------------------------------------------------------------
  # 2. Fetch Passaging rows for the initial subtree
  # -------------------------------------------------------------------------
  passaging_rows <- .db_fetch(
    paste0("SELECT * FROM `Passaging` WHERE `id` IN (",
           paste(DBI::dbQuoteString(conn, export_ids), collapse = ", "),
           ") ORDER BY `id`"),
    conn = conn
  )

  # -------------------------------------------------------------------------
  # 3. Apply external parent policy.
  # export_ids and passaging_rows are replaced with the policy results here.
  # All downstream fetches use these final values, so policy="include" correctly
  # expands the dependency / perspective / storage closures too.
  # -------------------------------------------------------------------------
  policy_result        <- .apply_external_parent_policy(
    passaging_rows         = passaging_rows,
    export_ids             = export_ids,
    policy                 = external_parent_policy,
    conn                   = conn
  )
  passaging_rows       <- policy_result$passaging_rows
  export_ids           <- policy_result$export_ids
  detached_parent_refs <- policy_result$detached_parent_refs

  # -------------------------------------------------------------------------
  # 4. Dependency closure — uses final passaging_rows (step 3 output)
  # -------------------------------------------------------------------------
  deps <- .fetch_dependency_closure(passaging_rows, conn = conn)

  # -------------------------------------------------------------------------
  # 5. Perspective + Loci closure — uses final export_ids (step 3 output)
  # -------------------------------------------------------------------------
  if (include_perspectives) {
    persp <- .fetch_perspective_closure(export_ids, conn = conn)
  } else {
    persp <- list(perspectives = data.frame(), loci = data.frame())
  }

  # -------------------------------------------------------------------------
  # 6. Storage rows — uses final export_ids (step 3 output)
  # -------------------------------------------------------------------------
  if (include_storage) {
    storage <- .fetch_storage_rows(export_ids, conn = conn)
  } else {
    storage <- list(liquid_nitrogen = data.frame(),
                    minus80_freezer = data.frame())
  }

  # -------------------------------------------------------------------------
  # 7. Decode molecular profiles (rds format only).
  # Shallow decode runs by default (decode_profiles_recursive=FALSE).
  # Recursive decode is not yet implemented (decode_profiles_recursive=TRUE
  # will stop with an informative error before reaching here).
  # Raw tables in `persp` are never mutated. Partial failures are captured in
  # decode_warnings and do not abort the export.
  # -------------------------------------------------------------------------
  decoded         <- NULL
  decode_warnings <- character(0)
  if (format == "rds") {
    if (decode_profiles_recursive) {
      stop(
        "decode_profiles_recursive=TRUE is not yet implemented. ",
        "The default shallow decode (decode_profiles_recursive=FALSE) ",
        "decodes all internal nodes present in the exported Perspective table. ",
        "Full recursive decoding of nodes not present in the export is a planned ",
        "future feature. See docs/specs/subtree_download/implementation_plan.md §D.7."
      )
    }
    decoded_result  <- .decode_perspective_profiles(persp$perspectives)
    decoded         <- decoded_result$profiles
    decode_warnings <- decoded_result$warnings
  }

  # -------------------------------------------------------------------------
  # Assemble the core bundle (same for both formats).
  # tables holds actual data frames in FK-safe order.
  # -------------------------------------------------------------------------
  pkg_ver <- tryCatch(as.character(packageVersion("cloneid")), error = function(e) "unknown")

  bundle <- list(
    metadata = list(
      root_id                = id,
      format                 = format,
      recursive              = recursive,
      include_storage        = include_storage,
      include_perspectives   = include_perspectives,
      decode_profiles_recursive = decode_profiles_recursive,
      external_parent_policy = external_parent_policy,
      exported_at            = Sys.time(),
      package_version        = pkg_ver,
      decode_warnings        = decode_warnings
    ),
    export_ids           = export_ids,
    detached_parent_refs = detached_parent_refs,
    tables               = list(
      CellLinesAndPatients          = deps$cell_lines,
      MediaIngredients              = deps$media_ingredients,
      Media                         = deps$media,
      Flask                         = deps$flasks,
      Loci                          = persp$loci,
      Passaging                     = passaging_rows,
      Perspective                   = persp$perspectives,
      LiquidNitrogen                = storage$liquid_nitrogen,
      Minus80Freezer                = storage$minus80_freezer
    ),
    decoded    = decoded,
    sql        = NULL,
    statements = NULL
  )

  # -------------------------------------------------------------------------
  # SQL format: build INSERT statements and formatted dump; augment bundle.
  # -------------------------------------------------------------------------
  if (format == "sql") {
    cl_stmts  <- .rows_to_inserts("CellLinesAndPatients",          deps$cell_lines,         conn)
    mi_stmts  <- .rows_to_inserts("MediaIngredients",              deps$media_ingredients,  conn)
    med_stmts <- .rows_to_inserts("Media",                         deps$media,              conn)
    fl_stmts  <- .rows_to_inserts("Flask",                         deps$flasks,             conn)
    lo_stmts  <- .rows_to_inserts("Loci",                          persp$loci,              conn,
                                  blob_cols = "content")
    pa_stmts  <- .rows_to_inserts("Passaging",                     passaging_rows,          conn)
    pe_stmts  <- .rows_to_inserts("Perspective",                   persp$perspectives,      conn,
                                  blob_cols = "profile")
    ln_stmts  <- .rows_to_inserts("LiquidNitrogen",                storage$liquid_nitrogen, conn)
    m8_stmts  <- .rows_to_inserts("Minus80Freezer",                storage$minus80_freezer, conn)

    insert_stmts <- c(cl_stmts, mi_stmts, med_stmts, fl_stmts,
                      lo_stmts, pa_stmts, pe_stmts,
                      ln_stmts, m8_stmts)

    bundle$statements <- c("START TRANSACTION;", insert_stmts, "COMMIT;")

    .section <- function(label, stmts) {
      if (length(stmts) == 0) return(character(0))
      c(paste0("\n-- ", label), stmts)
    }

    detach_summary <- if (nrow(detached_parent_refs) > 0) {
      paste(apply(detached_parent_refs, 1, function(r) {
        paste0(r["passaging_id"], ".", r["field"], " (was: ", r["original_value"], ")")
      }), collapse = "; ")
    } else {
      "none"
    }

    header <- paste(c(
      "-- CLONEID subtree export",
      paste0("-- root_id:                ", id),
      paste0("-- format:                 ", format),
      paste0("-- recursive:              ", recursive),
      paste0("-- include_storage:        ", include_storage),
      paste0("-- include_perspectives:   ", include_perspectives),
      paste0("-- external_parent_policy: ", external_parent_policy),
      paste0("-- exported_at:            ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
      paste0("-- package_version:        ", pkg_ver),
      paste0("-- exported_ids:           ", paste(sort(export_ids), collapse = ", ")),
      paste0("-- detached_parent_refs:   ", detach_summary)
    ), collapse = "\n")

    sql_body <- c(
      .section("CellLinesAndPatients",          cl_stmts),
      .section("MediaIngredients",              mi_stmts),
      .section("Media",                         med_stmts),
      .section("Flask",                         fl_stmts),
      .section("Loci",                          lo_stmts),
      .section("Passaging",                     pa_stmts),
      .section("Perspective",                   pe_stmts),
      .section("LiquidNitrogen",                ln_stmts),
      .section("Minus80Freezer",                m8_stmts)
    )

    bundle$sql <- paste(c(header, "", "START TRANSACTION;",
                          sql_body,
                          "", "COMMIT;"),
                        collapse = "\n")
  }

  # -------------------------------------------------------------------------
  # Write to file if requested
  # -------------------------------------------------------------------------
  if (!is.null(file)) {
    if (format == "rds") saveRDS(bundle, file)
    if (format == "sql") writeLines(bundle$sql, con = file)
  }

  bundle
}


#' Compatibility wrapper: export a subtree as a SQL dump.
#'
#' Thin wrapper around \code{export_passaging_subtree_bundle(..., format = "sql")}.
#' Kept for backwards compatibility.  Prefer \code{export_passaging_subtree_bundle()}.
#'
#' @inheritParams export_passaging_subtree_bundle
#' @return See \code{export_passaging_subtree_bundle}.
export_passaging_subtree_sql <- function(
    id,
    file                   = NULL,
    recursive              = FALSE,
    include_storage        = TRUE,
    include_perspectives   = TRUE,
    conn                   = NULL,
    external_parent_policy = c("nullify", "include", "error")) {
  export_passaging_subtree_bundle(
    id                     = id,
    file                   = file,
    format                 = "sql",
    recursive              = recursive,
    include_storage        = include_storage,
    include_perspectives   = include_perspectives,
    conn                   = conn,
    external_parent_policy = external_parent_policy
  )
}

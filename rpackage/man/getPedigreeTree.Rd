\name{getPedigreeTree}
\alias{getPedigreeTree}
\title{Phylogeny depicting how cell line lineages are related.}
\description{Visualizes relations between lineages of a cell line.}
\usage{getPedigreeTree(cellLine = cellLine, id = NULL, cex = 0.5, highlight = NULL, topology = c("internal", "legacy"), outputFile = NA_character_)}
\arguments{
  \item{cellLine}{
     Name of the cell line for which to plot the lineage tree. If set to NULL, next argument will be used.
  }
  \item{id}{
    ID of the lineage for which to plot the tree of all derived sublineages. This must be an existing key in the Passaging table.
  }
  \item{cex}{
    Text scaling passed to the tree plot.
  }
  \item{highlight}{
    Optional character vector of Passaging IDs to highlight. For each supplied ID, the plotted tree highlights the path from the plotted root to that node.
  }
  \item{topology}{
    Tree-construction mode. \code{"internal"} is the default and represents ancestor events as internal nodes. \code{"legacy"} preserves the older Newick construction where the root event is also rendered as a sibling tip.
  }
  \item{outputFile}{
    Output PDF path. By default, a timestamped PDF is written under \code{~/Downloads}. Set to \code{NULL} to disable automatic PDF output and draw only to the current graphics device.
  }
}
\details{
  IDs that are currently present in the \code{LiquidNitrogen} table are drawn in bold.
  When writing a PDF, the page width and height are estimated from tree depth, label count, label length, and per-depth label crowding.
}



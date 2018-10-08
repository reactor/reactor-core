#!/usr/bin/env bash
# For some reason reason svgo is not considering the disabled plugin options when I use the below shell. So using bash instead
# #!/bin/sh

inkscape=$(which inkscape)
svgo=$(which svgo)

projectRoot=`pwd`/../../../
rawMarblesDirectory="$projectRoot/docs/svg/marbles/raw"
optimizedMarblesDirectory="$projectRoot/reactor-core/src/main/java/reactor/core/publisher/doc-files/marbles"
intermediateSvgDirectory=/tmp/svg-text-to-path

if [ -d "$rawMarblesDirectory" ]; then
  if [ -x "$inkscape" ]; then
    if [ -x "$svgo" ]; then
      echo "### Removing all older optimized svgs ###";

      rm "$optimizedMarblesDirectory"/*.svg 2> /dev/null;
      rmdir "$optimizedMarblesDirectory" 2> /dev/null;
      mkdir "$optimizedMarblesDirectory";
      echo "### Cleared all older optimized svgs ###";

      echo "### Removing intermediate svg files from last run ###";
      rm "$intermediateSvgDirectory"/*.svg 2> /dev/null;
      rmdir "$intermediateSvgDirectory" 2> /dev/null;
      mkdir "$intermediateSvgDirectory";
      echo "### Removed all intermediate svg files ###";

      currentIndex=1;
      svgFileCount=`ls -l "$rawMarblesDirectory"/*.svg | grep -v ^l | wc -l`;

      echo "### Replacing text from original svgs with path ###";
      for f in "$rawMarblesDirectory"/*.svg;
      do
        echo "Converting (${currentIndex}/${svgFileCount}): ${f} -> $intermediateSvgDirectory/`basename $f .svg`.svg";
        inkscape --export-text-to-path --export-plain-svg="$intermediateSvgDirectory/`basename $f .svg`.svg" "$f" --without-gui;
        currentIndex=$((currentIndex+1));
      done
      echo "### Replaced text from original svgs with path ###";

      echo "### Optimizing svgs ###";
      svgo --multipass --disable={cleanupIDs,removeNonInheritableGroupAttrs} -f "$intermediateSvgDirectory" -o "$optimizedMarblesDirectory";
      echo "### Optimization complete ###";

      echo "### Cleaning up intermediate svg files ###";
      rm "$intermediateSvgDirectory"/*.svg 2> /dev/null;
      rmdir "$intermediateSvgDirectory" 2> /dev/null;
      echo "### Clean up complete ###";

      echo "### Conversion complete ###";
    else
      echo "svgo executable could not be found in PATH. Please add it to PATH variable and retry";
    fi
  else
    echo "inkscape executable could not be found in PATH. Please add it to PATH variable and retry";
  fi
else
  echo "Command must be run from within docs/svg/marbles sub directory of reactor-core repo"
fi

#!/usr/bin/env bash
# For some reason reason svgo is not considering the disabled plugin options when I use the below shell. So using bash instead
# #!/bin/sh

inkscape=$(which inkscape)
svgo=$(which svgo)

if [ -x "$inkscape" ]; then
  if [ -x "$svgo" ]; then
    echo "### Removing all older optimized svgs ###";
    rm optimized/*.svg;
    rmdir optimized;
    mkdir optimized;
    echo "### Cleared all older optimized svgs ###";

    echo "### Removing intermediate svg files from last run ###";
    rm svg-text-to-path/*.svg;
    rmdir svg-text-to-path;
    mkdir svg-text-to-path;
    echo "### Removed all intermediate svg files ###";

    currentIndex=1;
    svgFileCount=`ls -l "raw/"*.svg | grep -v ^l | wc -l`;

    echo "### Replacing text from original svgs with path ###";
    for f in "raw/"*.svg;
    do
      echo "Converting (${currentIndex}/${svgFileCount}): ${f} -> svg-text-to-path/`basename $f .svg`.svg";
      inkscape --export-text-to-path --export-plain-svg="svg-text-to-path/`basename $f .svg`.svg" "$f" --without-gui;
      currentIndex=$((currentIndex+1));
    done
    echo "### Replaced text from original svgs with path ###";

    echo "### Optimizing svgs ###";
    svgo --multipass --disable={cleanupIDs,removeNonInheritableGroupAttrs} -f svg-text-to-path -o optimized;
    echo "### Optimization complete ###";

    echo "### Cleaning up intermediate svg files ###";
    rm svg-text-to-path/*.svg;
    rmdir svg-text-to-path;
    echo "### Clean up complete ###";

    echo "### Conversion complete ###";
  else
    echo "svgo executable could not be found in PATH. Please add it to PATH variable and retry";
  fi
else
  echo "inkscape executable could not be found in PATH. Please add it to PATH variable and retry";
fi

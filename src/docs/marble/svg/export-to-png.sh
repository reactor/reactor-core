#!/bin/sh
echo "### Removing all older pngs ###";
for f in "../png/"*.png ;
do
 rm "$f";
done
echo "### Cleared old pngs ###";

echo "### Converting SVG -> PNG ###";
for f in *.svg ;
do
 inkscape "$f" --export-png="../png/`basename $f .svg`.png" --without-gui;
done
echo "### Conversion complete ###";

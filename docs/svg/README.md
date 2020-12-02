# Contributing SVGs

Most images are in SVG format and represent a marble diagram.
Such SVG are embedded in the javadoc jar, and as such need to be in a `doc-files` folder inside the source hierarchy.

As a result, most SVG live in `/reactor-core/src/main/java/reactor/core/publisher/doc-files/marbles/`.

The graphical conventions for SVG marble diagrams are displayed in `/docs/svg/conventions.svg` (in an unoptimized raw SVG).
One can pick graphical elements from it and add them to a new SVG marble diagram should one want to contribute such a diagram.

Some of these conventions have also been extracted and reworked with a user/reader's perspective in mind, in order to describe how to read a marble diagram.
These `legend-` SVGs are specific to the reference documentation.

All images that are specific to the reference documentation can be added to the `/docs/asciidoc/images/` folder.
SVGs purely aimed at the reference guide still need to be optimized, notably the `legend-` ones, otherwise they tend to have rendering issues in the PDF version.

## Contributing Marble Diagrams

The recommended workflow for editing or contributing new SVG marble diagrams is as follows:

 - Use a standard SVG editor (the cross-platform standalone editor we would recommend is [`Inkscape`](https://inkscape.org))
 - For main labels (eg. for the operator name in a box), use the `Tahoma` font family and fall back to `Nimbus Sans L` for Linux:
   - `font-family:'Tahoma','Nimbus Sans L'`
 - For labels that should look more like code, use `Lucida Console` font family and fallback to `Courier New`:
   - `font-family:'Lucida Console','Courier New'`
 - Preferably, run the new/edited SVG through an optimizer, but keep it pretty printed (see below)

All `*.svg` marble diagrams are found in `/reactor-core/src/main/java/reactor/core/publisher/doc-files/marbles/` folder.
This same folder will be embedded in the `-sources.jar` and `-javadoc.jar` artifacts.

Note that there is a `conventions.svg` document that regroups the different graphical conventions for representing recurrent elements of marbles (static operator vs method operator, blocking, timeouts, lambdas, etc...), as well as more SVGs in `/docs/asciidoc/images` (which also includes symlinks to `flux.svg` and `mono.svg` marbles).

### Optimizing the SVG files

We want the SVG size to be kept as small and clutter-free as possible, as the images are embedded in the source and javadoc jars.
As a result, we recommend that an optimization step be taken when editing/creating SVGs.

That said, in order for the SVG source to be readable in editors, we prefer the SVG to be kept pretty printed.

We also had a lot of problems with arrowheads disappearing during optimizations, which you should keep an eye out for.

So the recommended configuration for optimizing is as follows:

 - Use [svgo](https://github.com/svg/svgo) (it is an NPM module, but you can get it on OSX via Homebrew: `brew install svgo`)
 - Only optimize the file(s) you've been editing. For multiple files, prefer exporting the result in a temporary folder for verification first.
 - Use the following command from the repo root to maintain arrowheads and pretty printing while optimizing the SVGs correctly:
 
```sh
svgo --input="reactor-core/src/main/java/reactor/core/publisher/doc-files/marbles/reduce.svg" --multipass --pretty --indent=1 --precision=2 --disable={cleanupIDs,removeNonInheritableGroupAttrs,removeViewBox,convertShapeToPath}
```

TIP: you can use git to stage the file pre-optimization, check it renders correctly post-optimization and revert using `git checkout path/to/file.svg` if that is not the case.

Alternatively, to optimize a whole folder into a temporary folder for comparison, use the `--folder` and `--output` options:

```sh
svgo --folder="reactor-core/src/main/java/reactor/core/publisher/doc-files/marbles/" --multipass --pretty --indent=1 --precision=2 --disable={cleanupIDs,removeNonInheritableGroupAttrs,removeViewBox,convertShapeToPath} --output=/tmp/svg/ --quiet
```

### Optimization Tips and Tricks

#### Arrows and other elements disappear
Try ungrouping the object.
Some objects are grouped for copy/paste convenience in the `conventions.svg` document, but should be ungrouped.

### Inkscape Tips and Tricks

#### On OSX with dual monitor Inkscape 0.x disappear
Unfortunately, Inkscape with XQuartz is not always ideally working. This disappearing problem is due to misalignment in coordinate systems.

You can deactivate an option in OSX's `Mission Control` preferences: uncheck `Displays have separate spaces`.
This needs a re-login and has the unfortunate effect that the menu bar will now only appear on the main screen (the one with the dock).

#### Inkscape 0.x is slow as hell to open and close documents
 - Try closing all but one document, then close every tool dialog and restart Inkscape
 - In Preferences, you can try playing with `Rendering` options: number of thread, cache size, display filters quality.

### How to reduce a rounded rectangle (buffers)
Double click on the rectangle and select the top left corner (square handle).
Hold Ctrl (or Cmd) to only move horizontally, and move the handler closer to where you want it.
Repeat the same operation with the bottom-right handle.
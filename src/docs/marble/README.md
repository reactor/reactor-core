## Prerequisites

1. Install [IBM Plex Sans](https://fonts.google.com/specimen/IBM+Plex+Sans) font on your local machine
2. Install [Inkscape](https://inkscape.org)
3. Make sure you installed [svgo](https://github.com/svg/svgo)
  1. Installed [yarn](https://yarnpkg.com)
  2. Install `svgo` by running `yarn global add svgo` from terminal

## Change workflow

> NOTE: No files inside `optimized` folder of `<root>/reactor-core/src/docs/marble` should be edited manually as this folder should be regenerated manually after the marbles in `raw` folder are edited

1. Update marbles in `<root>/reactor-core/src/docs/marble/raw` \[Dont touch any file anything inside `optimized` folder\]
2. Rebuild files inside `optimized` by following below steps

## Optimize SVGs

1. Run `cd <path>/reactor-core/src/docs/marble && ./optimize.sh && cd -` from terminal
2. You should see the optimized svgs in `<path>/reactor-core/src/docs/marble/optimized` directory

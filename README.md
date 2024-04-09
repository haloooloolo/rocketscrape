# RocketScrape

A command line tool to process Discord message streams.

## Setup

To get started, install the `rocketscrape` Python package from within the directory using:
```bash
pip install -e .
```
RocketScrape requires a Discord authentication token for API requests, which should be stored in a
`DISCORD_USER_TOKEN` environment variable. For more information on how to obtain a token for your account see https://discordpy-self.readthedocs.io/en/latest/authenticating.html.
```bash
export DISCORD_USER_TOKEN=<token>
```

## Usage

The tool primarily expects a source (flag) and analysis type (positional argument) to be specified.
Below are examples of how to use RocketScrape:

```bash
rocketscrape -c CHANNEL1 [-c CHANNEL2, ...] (--include-threads | --exclude-threads) ANALYSIS
```
Replace `CHANNEL1`, `CHANNEL2`, etc., with either the Discord channel / thread ID or one of the predefined channel names from
the `Channel` enum in [rocketscrape.py](src/rocketscrape.py). You can choose to include or exclude threads within specified
channels using the required `--include-threads` or `--exclude-threads` flag. Similarly, you can run rocketscrape on an entire server using:
```bash
rocketscrape --server SERVER (--include-threads | --exclude-threads) ANALYSIS
```
`SERVER` can either be a server ID or a name defined in the `Server` enum. There are optional `-s / --start` and
`-e / --end` arguments that accept an ISO format datetime string to restrict the date range. For instance,
to get the top contributors for the support channel over the last month, you can use:
```bash
rocketscrape -c support -s $(date -d "-1 month" +"%Y-%m-%d") --include-threads contributors
```
For a complete list of global options and analysis types, run the help command:
```bash
rocketscrape -h
```
Additionally, you can access help commands for specific analysis types, e.g.:
```bash
rocketscrape contributor-history -h
```

## Adding a custom analysis class
Each analysis type in RocketScrape is implemented as a subclass of `MessageAnalysis`. These classes are required to
implement `_require_reactions(self)`, `_prepare(self)`, `_on_message(self, message)`, `_finalize(self)`,
`_display_result(self, result, client, max_results)` and `subcommand()`. Optionally, `custom_args(cls)` can be overridden
to add analysis-specific command line arguments. Examples can be found in [analysis.py](src/analysis.py).
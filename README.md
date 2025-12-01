# 🔋 BESS-INTRA-TRADING: Rolling Intrinsic Optimization for Intraday BESS Trading

`bess-intra-trading` is a specialized **Python library** that provides a robust, modular implementation of the 
**Rolling Intrinsic (RI) optimization strategy** for Battery Energy Storage System (BESS) **intraday energy arbitrage**.

## Overview 

`bess-intra-trading` formalizes and extends the theory presented in [1], offering a clean, object-oriented framework 
designed for:
* **Data Handling:** Automated ingestion and preparation of high-frequency intraday market data (e.g., EPEX Spot).
* **Optimization Core:** Formulation and solution of the underlying **Mixed-Integer Linear Program (MILP)** using a 
state-of-the-art solver.
* **Simulation:** A complete `RollingIntrinsicStrategy` class for fast, configurable backtesting and performance analysis.

## Key features

[//]: # (All features and functionality are fully-customisable:)

[//]: # ()
[//]: # (* Prepare and submit DFT calculations to SLURM clusters from a single structure or composition input.)

[//]: # ()
[//]: # (* Automated relaxation workflows that help localize polarons on specific atomic sites.)

[//]: # ()
[//]: # (* Post-processing tools to extract:)

[//]: # ()
[//]: # (  * polaron formation energies)

[//]: # ()
[//]: # (  * atomic charges &#40;Bader / Hirshfeld / Mulliken&#41;)

[//]: # ()
[//]: # (  * local magnetic moments )

[//]: # ()
[//]: # (* Automated NEB generation for polaron migration pathways and barrier estimation.)

[//]: # ()
[//]: # (* Utilities to collect and format results into datasets for ML potential fitting.)

[//]: # ()
[//]: # (* Flexible: works with common DFT packages through configurable templates &#40;VASP, FHI-AIMS&#41;.)

## Installation

1. Create a `conda` environment with the command:

`conda create -n bess_intra_trading`

2. Download the `bess-intra-trading` repository:

```bash
git clone https://github.com/mttrin93/bess-intra-trading.git
cd bess_intra_trading
```

3. Run installation script:

`pip install .`

to install the package via `pip`. Alternatively, run the command:

`pip install .[dev]`

to install the package and tests used for development. 
Now, the `create_data`,  scripts can be used to run the trading workflow.

**Script overview:**

* `create_data` — set up the PostgreSQL database and either create randomized transaction data or use actual EPEX Spot 
data (if available)

[//]: # (* `run_attractor` — generates DFT input files and submission scripts, and optionally submits polaron relaxation)

[//]: # (jobs using the electron attractor method [3])

[//]: # ()
[//]: # ()
[//]: # (* `run_pbeu_plus_hybrid` — generates DFT input files and submission scripts, and optionally submits polaron relaxation)

[//]: # (jobs performing a combination of DFT+U and hybrid-functional relaxations.)

## Literature

The present implementation of the Rolling Intrinsic BESS Intraday Trading strategy can be found in the following paper:

[1] [An Algorithm for Modelling Rolling Intrinsic Battery Trading on the Continuous Intraday Market](https://dl.acm.org/doi/abs/10.1145/3717413.3717428)

## Running workflow (CLI executables)

The `create_data` executables can be run with the following options:

* `--db-name`: PostgreSQL database name.
* `--db-user`: PostgreSQL user.
* `--db-password`: PostgreSQL password.
* `--db-host`: PostgreSQL host.
* `--db-port`: PostgreSQL port.
* `--num-rows`: Number of fake transactions to generate (e.g., 1000000).
* `--file-path`: Path to an external data file (e.g., CSV) to load instead of generating fake data.

A common example command for dataset creation, would be:

```bash
create_data --num-rows 10000 
 ```

[//]: # ()
[//]: # (where the calculation is automatically submitted to the SLURM cluster.)

[//]: # ()
[//]: # (The `run_attractor` executable can be run with the following additional options:)

[//]: # ()
[//]: # (* `-ae, --attractor-elements`: Element symbol used to substitute the host atom to create the )

[//]: # (localized potential well.)


## Development & testing

Run unit tests with pytest:

`pytest tests/`

Use the included linters and formatters (e.g. `black` and `flake8`) to maintain code quality.

## Contributing

Contributions, suggestions and bug reports are very welcome! Please open issues or pull requests. Consider adding tests and 
updating documentation for non-trivial changes.

## Citation

If you use `bess-intra-trading` in published work, please cite the repository.

## License

This project is licensed under the MIT License — see the LICENSE file for details.

## Contact

Maintainer: Matteo Rinaldi — rinaldim1993@gmail.com
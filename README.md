# 🔋 BESS-INTRA-TRADING: Rolling Intrinsic Optimization for Intraday BESS Trading

`bess_intra_trading` is a specialized **Python library** that provides a robust, modular implementation of the 
**Rolling Intrinsic (RI) optimization strategy** for Battery Energy Storage System (BESS) **intraday energy arbitrage**.

This package formalizes and extends the theory presented in [1], offering a clean, object-oriented framework designed for:
* **Data Handling:** Automated ingestion and preparation of high-frequency intraday market data (e.g., EPEX Spot).
* **Optimization Core:** Formulation and solution of the underlying **Mixed-Integer Linear Program (MILP)** using a state-of-the-art solver.
* **Simulation:** A complete `RollingIntrinsicStrategy` class for fast, configurable backtesting and performance analysis.

## Overview 

[//]: # (`pypolaron` is a high-throughput framework to automate density-functional-theory &#40;DFT&#41; calculations on SLURM clusters, starting from )

[//]: # (an input structure or composition. The framework focuses on workflows for polaron localisation and migration: it can prepare and )

[//]: # (submit relaxation jobs that encourage polarons to localize on chosen atomic sites, perform post-processing to extract polaron )

[//]: # (formation energies, charges and magnetic moments, and generate nudged-elastic-band &#40;NEB&#41; calculations to estimate migration barriers. )

[//]: # (The produced data can be assembled into datasets suitable for training polaron-aware machine-learning interatomic potentials.)

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

2. Download the `bess_intra_trading` repository:

```bash
git clone https://github.com/mttrin93/pypolaron.git
cd pypolaron
```

3. Run installation script:

`pip install .`

to install the package via `pip`. Alternatively, run the command:

`pip install .[dev]`

to install the package and tests used for development. 
Now, the `run_generator`, `run_attractor` and `run_pbeu_plus_hybrid` scripts can be used to run the polaron workflows.

**Script overview:**

[//]: # (* `run_generator` — generates DFT input files and submission scripts, and optionally submits polaron calculations )

[//]: # (jobs &#40;relaxations, single scf runs&#41;.)

[//]: # ()
[//]: # (* `run_attractor` — generates DFT input files and submission scripts, and optionally submits polaron relaxation)

[//]: # (jobs using the electron attractor method [3])

[//]: # ()
[//]: # (* `run_pbeu_plus_hybrid` — generates DFT input files and submission scripts, and optionally submits polaron relaxation)

[//]: # (jobs performing a combination of DFT+U and hybrid-functional relaxations.)

## Literature

The present implementation of the Rolling Intrinsic BESS Intraday Trading strategy can be found in the following paper:

[1] [An Algorithm for Modelling Rolling Intrinsic Battery Trading on the Continuous Intraday Market](https://dl.acm.org/doi/abs/10.1145/3717413.3717428)

[//]: # ([2] [Finite-size corrections of defect energy levels involving ionic polarization]&#40;https://journals.aps.org/prb/abstract/10.1103/PhysRevB.102.041115&#41;)

[//]: # ()
[//]: # ([3] [Efficient Method for Modeling Polarons Using Electronic Structure Methods]&#40;https://pubs.acs.org/doi/10.1021/acs.jctc.0c00374&#41;)

[//]: # ()
[//]: # ([4] [Polarons in materials]&#40;https://www.nature.com/articles/s41578-021-00289-w&#41;)

## Running workflow (CLI executables)

[//]: # (The `run_generator`, `run_attractor` and `run_pbeu_plus_hybrid` executables can be run with the following options:)

[//]: # ()
[//]: # (* `-f, --file`: path to a structure file &#40;POSCAR, CIF, geometry.in, structure.xyz&#41;. Use )

[//]: # (this to run workflows starting from a local structure.)

[//]: # ()
[//]: # (* `-mq, --mp-query`: query the Materials Project by ID &#40;e.g. `mp-2657`&#41; or by composition )

[//]: # (&#40;e.g. TiO2&#41;.)

[//]: # ()
[//]: # (* `-mak, --mp-api-key`: Materials Project API Key.)

[//]: # ()
[//]: # (* `-l, --log`: set a log filename.)

[//]: # ()
[//]: # (* `-rdr, --run-dir-root`: root path where workflow run directories are created.)

[//]: # ()
[//]: # (* `-ds, --do-submit`: enable submission of generated job scripts to the scheduler immediately.)

[//]: # ()
[//]: # (* `-rp, --run-pristine`: run the pristine &#40;undefected&#41; structure, useful for formation )

[//]: # (energy references.)

[//]: # ()
[//]: # (* `-pt, --polaron-type`: electron or hole.)

[//]: # ()
[//]: # (* `-pn, --polaron-number`: number of extra polarons to add.)

[//]: # ()
[//]: # (* `-ovn, --oxygen-vacancy-number`: number of oxygen vacancies to create.)

[//]: # ()
[//]: # (* `-scf, --setup-config-file`: path to the YAML file containing all DFT and defect generation settings.)

[//]: # ()
[//]: # (* `-pf, --policy-file`: path to a YAML file containing workflow execution policy settings.)

[//]: # ()
[//]: # (A more detailed description of the single options can be obtained running `run_generator -h`. )

[//]: # ()
[//]: # (A common example command for relaxing an electron polaron in MgO using `aims`, with the )

[//]: # (structure fetched from Materials Project, would be:)

[//]: # ()
[//]: # (```bash)

[//]: # (run_generator \)

[//]: # (  -mq MgO \)

[//]: # (  -mak ID \)

[//]: # (  -rdr ./workdir \)

[//]: # (  -ds \)

[//]: # (  -scf /path/to/examples/dft_setup.yaml \)

[//]: # (  -pf /path/to/examples/policy.yaml)

[//]: # ( ```)

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

If you use `bess_intra_trading` in published work, please cite the repository.

## License

This project is licensed under the MIT License — see the LICENSE file for details.

## Contact

Maintainer: Matteo Rinaldi — rinaldim1993@gmail.com
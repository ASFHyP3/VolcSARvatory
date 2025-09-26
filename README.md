# VolcSARvatory

This repository contains the recipe to process multiburst InSAR pairs for an SBAS network for an area of interest.

## Installation

1. Install Anaconda or Miniconda.
2. Clone the `VolcSARvatory` repository.
   ```bash
   git clone https://github.com/ASFHyP3/VolcSARvatory.git
   ```
3. Clone a modified version of `asf-search`.
   ```bash
   git clone https://github.com/mfangaritav/Discovery-asf_search.git
   ```
4. setup the development environment
   ```bash
    conda env create -f VolcSARvatory/environment.yaml
   ```
5. Activate `volcsarvatory` environment:
   ```bash
   conda activate volcsarvatory
   ```
6. Install the modified version of `asf-search`:
   ```bash
   python -m pip install -e Discovery-asf_search/
   ```
7. Install `volcsarvatory`:
   ```bash
   python -m pip install -e VolcSARvatory/
   ```

## Usage
The notebook [`VolcSARvatory`](https://github.com/ASFHyP3/VolcSARvatory/VolcSARvatory.ipynb) includes an example in Kilauea to submit an SBAS network in HyP3.

### Credentials
Depending on the mission being processed, some workflows will need you to provide credentials. Generally, credentials are provided via environment variables, but some may be provided by command-line arguments or via a `.netrc` file. 

## Code of conduct
We strive to create a welcoming and inclusive community for all contributors to HyP3 autoRIFT. As such, all contributors to this project are expected to adhere to our code of conduct.

Please see our [`CODE_OF_CONDUCT.md`](https://github.com/ASFHyP3/.github/blob/main/CODE_OF_CONDUCT.md) for the full code of conduct text.

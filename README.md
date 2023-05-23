# qc_through_sensorthings

## Description
The goal of this project is to perform basic quality checks on data.
The project adds quality flags to the database (if configured).
The project should/will be structured that adding a new algorithm is as simple as changing a config file (yaml).

## Installation
As it is a python project, no *real* installation is needed, but a runtime python environment needs to be created where the needed packages are available.
The needed packages are listed in the file `requirements.txt`.

### Linux
When working on a linux kernel, the setup and the running of the project can be done through `make`.

### Windows
No install script if foreseen for windows.

## Usage

## Roadmap

## License

## Project status


### inspect return obj restructure

- out -> dict
  - Thing
    - name
    - @iot.id
  - Datastreams
    - name
      - @iot.id
      - unitOfMeasurement
    - ObservedProperty
      - name
        - @iot.id
    - Observations
      - count

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

## TODO

- [ ] Patch on test db
- [ ] REF iterrows() -> function on columns
- [ ] filter on date/time/...
- [ ] Z needed in ISO time
- [ ] put flag to BAD 
- [ ] 160, 170 first sensors to focus on
  - waterflow is first one to check
- [ ] geolocation
  - feature of interest -> is the point close to the one before
  - one location is shared by all datastreams, can be done for all

## Questions

- observedArea
  - are the coordinates based on the coordinates of the observations? or preset? If preset, could be checked if in box
  - what coordinate reference system is used?
  - 

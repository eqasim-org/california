# How to create a scenario

The following sections describe three steps of using the pipeline. To generate
the synthetic population, first all necessary data must be gathered. Afterwards,
the pipeline can be run to create a synthetic population in *CSV* and *GPKG*
format. Optionally, the pipeline can then prepare a [MATSim](https://matsim.org/)
simulation and run it in a third step:

- [Gathering the data](#section-data)
- [Running the pipeline](#section-data)
- *(Optional)* [Running the simulation](#section-simulation)

## <a name="section-data"></a>Gathering the data

To create the scenario, a couple of data sources must be collected. It is best
to start with an empty folder, e.g. `/data`. All data sets need to be named
in a specific way and put into specific sub-directories. The following paragraphs
describe this process.

The main file used to configure the pipeline is called `config.yml`.

### 1) Census data (Censo 2010)

Census data containing the socio-demographic information of people living in
USA is available through the USA Census:

- [Census data](http://census.gov)
- The census data is available either as marginal distributions for certain variables or as microdata samples (PUMS)
- To create a representative set of individuals and households characterized with certain attributes we use PopGen as an example framework
- The `data/census/prepare_popgen.py` stage creates the input PopGen files that you can use to generate the necessary files for later stages of the pipeline
- As an input to this stage you will need PUMS dataset, which can be obtained form [here](https://www2.census.gov/programs-surveys/acs/data/pums/). You can downloaded the latest 5-year estimates for California. Currently located [here](https://www2.census.gov/programs-surveys/acs/data/pums/2019/5-Year/)
- You need to download two zip files: `csv_hca.zip` and `csv_pca.zip` containing houshold and population samples, and unzip them in the `popgen_input_path` which you can deifne within the `config.yml` file. These two files are called `psam_p06.csv` and `psam_h06.csv`
- The PopGen will create two files: `full_population.csv` and `full_households.csv`. You need to place these files in the right folder under `/data/census`
- PopGen v2.0 can be obtained from [here](https://www.mobilityanalytics.org/popgen.html). Unfortunately, the tool is a bit old and only works with Python 2. We advise setting up an environment with Python 2 to be able to execute this stage. 
- An alternative to using PopGen is to replace data/census/prepare_popgen and data/census/cleaned stages with the [PopulationSim](https://activitysim.github.io/populationsim/). However, we have not tested this approach yet.

### 2) California household travel survey 

The California household travel survey is available from NREL:

- [California household travel survey](https://www.nrel.gov/transportation/secure-transportation-data/tsdc-california-travel-survey.html)
- Download Full Survey Data
- Put the downloaded contents of the *zip* file int othe folder `/data/HTS/`.

### 3) OpenStreetMap data

The OpenStreetMap data is avaialble from Geofabrik:

- Here you can find [North California OSM](http://download.geofabrik.de/north-america/us/california/norcal.html) data
- Here you can find [South California OSM](http://download.geofabrik.de/north-america/us/california/socal.html) data
- Download the norcal-latest.osm.pbf (or socal-latest.osm.pbf) file
- Now you need to cut-out the region you want to study. 
This can be easily done using a tool called [osmosis](https://github.com/openstreetmap/osmosis). Download the latest build and follow
the installation instructions. You would need a polygon 
file describing the boundaries of the region, which you can find also [here](../resources); sf.poly is provided as example how this should look like for San Francisco nine-county area. 
- The following command using osmosis can be used to cut-out a San Francisco region:
```
osmosis --read-pbf file="norcal-latest.osm.pbf" --bounding-polygon file="ss.poly" --write-pbf file="sf_bay.osm.pbf"
```
- Cut-out San Francisco or other region and place the generated file, in our case sf_bay.osm.pbf file in `data/osm`

### 4) Educational facilities

The private and public schools in California are obtained as well from:
- [Public Schools California](https://www.cde.ca.gov/ds/si/ds/pubschls.asp); here you can download a txt file containing Public Schools and Districts
- [Postsecondary Schools in California](https://catalog.data.gov/dataset/postsecondary-school-locations-current); download the csv file, rename it to Colleges_and_Universities.csv
- Place the downloaded *csv* files in `data/education/`.

### 5) Census zoning system

The Census zoning system is available on different levels. However we use census tract as the zone of aggregation:

- You can find them under resources folder for San Francisco Bay Area and Los Angeles Area. Please copy the files to `data/spatial` depending which area you want to work with.
- The files provided here ensure that the studied area does not contain islands, as MATSim structure does not allow it. It also contains specific shapefiles (i.e., `SF_InnerCity`), which are used to impute specific attributes to the population.
- All these files are created based on the zoning file, which can be obtained [here](https://www2.census.gov/geo/tiger/TIGER2017/TRACT/), file called `tl_2017_06_tract.zip`. These files are periodically updated, and currently the newest one is from 2019.

### 6) Commuting data

Commuting data is obtained from the Ammerican Community Survey (ACS). 
- In the current pipeline we use `B302201` table for the census tract to census tract flows. Other tabulations can be used as well with some adaptations of the code.
- The code also requires the documentation of the CTPP dataset, which can be obtained from the [ftp server](ftp:\\data5.ctpp.transportation.org)
- The documentation should be unzipped and placed next to the `B302201` in the `data/CTPP` directory


### 7) *(Optional)* Road network (OpenStreetMap)

Only in case you want to run a full simulation of the scenario (rather than
creating the synthetic population in itself), you need to use the sao-paulo osm data again:

- The file you have created in step 3 unpack to .osm (you can do that using osmosis tool):
```
osmosis --read-pbf file="sf_bay.osm.pbf" --write-xml file="sf_bay.osm"
```
- In order to save storage space, you should pack it to sf_bay.osm.gz
- Put the *gz* file into the folder `data/osm`.

### 8) *(Optional)* Public transit schedule (GTFS)

Again, only if you want to run a full simulation, you need to download the
public transit schedules. There are many transit agencies in the area and this process can be very time consuming:

- You can get the transit agencies files from resources/sf/transit
- Or alternatively you can download the current GTFS schedules and place them in the `data/gtfs` folder
- If you choose to download current GTFS files you will need to adapt the gtfs_merger stage to take into account the number and namings of the gtfs files you have downlaoded
- If you are using provided files, you do not ahve to do anything
- 
### Overview

Your folder structure should now have at least the following files for the San Francisco example:

- `data/CHTS/survey_person.csv`
- `data/CHTS/survey_activity.csv`
- `data/CHTS/survey_place.csv`
- `data/CHTS/survey_households.csv`
- `data/population/psam_p06.csv`
- `data/population/psam_h06.csv`
- `data/population/full_population.csv` after generating it with PopGen
- `data/population/full_households.csv` after generating it with PopGen
- `data/CTPP/CA_2012thru2016_B302201.csv`
- `data/CTPP/2012-2016 CTPP documentation/*`
- `data/education/pubschls.txt`
- `data/education/Colleges_and_Universities.csv`
- `data/Spatial/SF_InnerCity.cpg`
- `data/Spatial/SF_InnerCity.shp`
- `data/Spatial/SF_InnerCity.dbf`
- `data/Spatial/SF_InnerCity.prj`
- `data/Spatial/SF_InnerCity.shx`
- `data/Spatial/SF_Bay_Area_cleaned.cpg`
- `data/Spatial/SF_Bay_Area_cleaned.shp`
- `data/Spatial/SF_Bay_Area_cleaned.dbf`
- `data/Spatial/SF_Bay_Area_cleaned.prj`
- `data/Spatial/SF_Bay_Area_cleaned.shx`
- `data/osm/sf_bay.osm.pbf`

If you want to run the simulation, there should be also the following files (similar if you want to build any other region in California):

- `data/osm/sf_bay.osm.gz` 
- `data/gtfs/*`

## <a name="section-population">Running the pipeline

The pipeline code is available in [this repository](https://github.com/eqasim-org/california).
To use the code, you have to clone the repository with `git`:

```bash
git clone https://github.com/eqasim-org/california
```

which will create the `california` folder containing the pipeline code. To
set up all dependencies, especially the [synpp](https://github.com/eqasim-org/synpp) package,
which is the code of the pipeline code, we recommend setting up a Python
environment using [Anaconda](https://www.anaconda.com/):

```bash
cd california
conda env create -f environment.yml
```

This will create a new Anaconda environment with the name `california`. (In
case you don't want to use Anaconda, we also provide a `requirements.txt` to
install all dependencies in a `virtualenv` using `pip install -r requirements.txt`).

To activate the environment, run:

```bash
conda activate california
```

Now have a look at `config.yml` which is the configuration of the pipeline.
Check out [synpp](https://github.com/eqasim-org/synpp) to get a more general
understanding of what it does. For the moment, it is important to adjust
two configuration values inside of `config.yml`:

- `working_directory`: This should be an *existing* (ideally empty) folder where
the pipeline will put temporary and cached files during runtime.
- `data_path`: This should be the path to the folder where you were collecting
and arranging all the raw data sets as described above.
- `output_path`: This should be the path to the folder where the output data
of the pipeline should be stored. It must exist and should ideally be empty
for now.

To set up the working/output directory, create, for instance, a `cache` and a
`output` directory. These are already configured in `config.yml`:

```bash
mkdir cache
mkdir output
```

Everything is set now to run the pipeline. The way `config.yml` is configured
it will create the relevant output files in the `output` folder.

To run the pipeline, call the [synpp](https://github.com/eqasim-org/synpp) runner:

```bash
python3 -m synpp
```

It will automatically detect the `config.yml`, process all the pipeline code
and eventually create the synthetic population. You should see a couple of
stages running one after another. Most notably, first, the pipeline will read all
the raw data sets to filter them and put them into the correct internal formats.

After running, you should be able to see a couple of files in the `output`
folder:

- `meta.json` contains some meta data, e.g. with which random seed or sampling
rate the population was created and when.
- `persons.csv` and `households.csv` contain all persons and households in the
population with their respective sociodemographic attributes.
- `activities.csv` and `trips.csv` contain all activities and trips in the
daily mobility patterns of these people including attributes on the purposes
of activities or transport modes for the trips.
- `activities.gpkg` and `trips.gpkg` represent the same trips and
activities, but in the spatial *GPKG* format. Activities contain point
geometries to indicate where they happen and the trips file contains line
geometries to indicate origin and destination of each trip.

## <a name="section-simulation">Running the simulation

The pipeline can be used to generate a full runnable [MATSim](https://matsim.org/)
scenario and run it for a couple of iterations to test it. For that, you need
to make sure that the following tools are installed on your system (you can just
try to run the pipeline, it will complain if this is not the case):

- **Java** needs to be installed, with a minimum version of Java 8 (11 would be advisable). In case
you are not sure, you can download the open [AdoptJDK](https://adoptopenjdk.net/).
- **Maven** needs to be installed to build the necessary Java packages for setting
up the scenario (such as pt2matsim) and running the simulation. Maven can be
downloaded [here](https://maven.apache.org/) if it does not already exist on
your system.
- **git** is used to clone the repositories containing the simulation code. In
case you clone the pipeline repository previously, you should be all set.

Then, open again `config.yml` and uncomment the `matsim.output` stage in the
`run` section. If you call `python3 -m synpp` again, the pipeline will know
already which stages have been running before, so it will only run additional
stages that are needed to set up and test the simulation.

You can choose currently between two possible runnable scenarios: los_angeles and san_francisco.
In the config.yml file you can choose one or the other using the eqasim_java_package parameters.
`eqasim_java_package: "san_francisco"` will run a San Francisco scenario using the
eqasim framework.

There are several other parameters that can be configured within the config.yml file:
- You can define for which counties you want to create the population; you have to define `counties` containing county IDs and `county_names`, containing their names
- You also have to define `zones` which contain three digit county codes
- `minimum_source_samples` is used int eh hot deck matching algorithm when activity chains are assigned to individuals
- `spatial_file` represents the census tracts that are contained within the area that you want to synthesize
- `spatial_imputation_file`, `spatial_imputation_file_la`, and `spatial_imputation_file_orange` are used for imputation of specific attributes that are used in the synthesize process
- `osm_file` can be used to define the name of the OSM file used in the synthesize process
- `osm_file_pt2matsim` can be used to define the file used in pt2matsim stage where the MATSim scenario is set up
- `data_path` is used to define the path to the `data` folder
- `output_path` is used to define the path to the output folder
- `popgen_input_path` is used to define the path to the folder where popgen stage input and output files are stored

After running, you should find the MATSim scenario files in the `output`
folder:

- `san_francisco_population.xml.gz` containing the agents and their daily plans.
- `san_francisco_facilities.xml.gz` containing all businesses, services, etc.
- `san_francisco_network.xml.gz` containing the road and transit network
- `san_francisco_households.xml.gz` containing additional household information
- `san_francisco_transit_schedule.xml.gz` and `san_francisco_transit_vehicles.xml.gz` containing public transport data
- `san_francisco_config.xml` containing the MATSim configuration values
- `san_francisco-1.2.1.jar` containing a fully packaged version of the simulation code including MATSim and all other dependencies

If you want to run the simulation again (in the pipeline it is only run for
two iterations to test that everything works), you can now call the following:

```bash
java -Xmx14G -cp san_francisco-1.2.1.jar org.eqasim.san_francisco.RunSimulation --config-path san_francisco_config.xml
```

This will create a `simulation_output` folder (as defined in the `san_francisco_config.xml`)
where all simulation is written.

import shutil
import os.path

import matsim.runtime.eqasim as eqasim

def configure(context):
    context.stage("matsim.scenario.population")
    context.stage("matsim.scenario.households")

    context.stage("matsim.scenario.facilities")
    context.stage("matsim.scenario.supply.processed")
    context.stage("matsim.scenario.supply.gtfs")

    context.stage("matsim.runtime.java")
    context.stage("matsim.runtime.eqasim")

    context.config("sampling_rate")
    context.config("processes")
    context.config("random_seed")
    context.config("data_path")

def execute(context):
    # Prepare input files
    facilities_path = "%s/%s" % (
        context.path("matsim.scenario.facilities"),
        context.stage("matsim.scenario.facilities")
    )

    population_path = "%s/%s" % (
        context.path("matsim.scenario.population"),
        context.stage("matsim.scenario.population")
    )

    network_path = "%s/%s" % (
        context.path("matsim.scenario.supply.processed"),
        context.stage("matsim.scenario.supply.processed")["network_path"]
    )
    
    sf_path = "%s/SF_Spatial/SF_InnerCity.shp" % context.config("data_path")

    eqasim.run(context, "org.eqasim.core.scenario.preparation.RunPreparation", [
        "--input-facilities-path", facilities_path,
        "--output-facilities-path", "san_francisco_facilities.xml.gz",
        "--input-population-path", population_path,
        "--output-population-path", "prepared_population.xml.gz",
        "--input-network-path", network_path,
        "--output-network-path", "san_francisco_network.xml.gz",
        "--threads", context.config("processes")
    ])
    
    eqasim.run(context, "org.eqasim.san_francisco.preparation.RunImputeInnerSFAttribute", [
            "--sf-path", sf_path,
            "--input-path", "prepared_population.xml.gz",
            "--output-path", "prepared_population.xml.gz"
        ])
        
    eqasim.run(context, "org.eqasim.san_francisco.scenario.RunNetworkFixer", [
            "--input-path", "san_francisco_network.xml.gz",
            "--output-path", "san_francisco_network.xml.gz"
        ])    

    assert os.path.exists("%s/san_francisco_facilities.xml.gz" % context.path())
    assert os.path.exists("%s/prepared_population.xml.gz" % context.path())
    assert os.path.exists("%s/san_francisco_network.xml.gz" % context.path())

    # Copy remaining input files
    households_path = "%s/%s" % (
        context.path("matsim.scenario.households"),
        context.stage("matsim.scenario.households")
    )
    shutil.copy(households_path, "%s/san_francisco_households.xml.gz" % context.cache_path)

    transit_schedule_path = "%s/%s" % (
        context.path("matsim.scenario.supply.processed"),
        context.stage("matsim.scenario.supply.processed")["schedule_path"]
    )
    shutil.copy(transit_schedule_path, "%s/san_francisco_transit_schedule.xml.gz" % context.cache_path)

    transit_vehicles_path = "%s/%s" % (
        context.path("matsim.scenario.supply.gtfs"),
        context.stage("matsim.scenario.supply.gtfs")["vehicles_path"]
    )
    shutil.copy(transit_vehicles_path, "%s/san_francisco_transit_vehicles.xml.gz" % context.cache_path)

    # Generate base configuration
    eqasim.run(context, "org.eqasim.core.scenario.config.RunGenerateConfig", [
        "--sample-size", context.config("sampling_rate"),
        "--threads", context.config("processes"),
        "--prefix", "san_francisco_",
        "--random-seed", context.config("random_seed"),
        "--output-path", "generic_config.xml"
    ])
    assert os.path.exists("%s/generic_config.xml" % context.path())

    # Adapt config for San Francisco
    eqasim.run(context, "org.eqasim.san_francisco.scenario.RunAdaptConfig", [
        "--input-path", "generic_config.xml",
        "--output-path", "san_francisco_config.xml"
    ])
    assert os.path.exists("%s/san_francisco_config.xml" % context.path())

    # Route population
    eqasim.run(context, "org.eqasim.core.scenario.routing.RunPopulationRouting", [
        "--config-path", "san_francisco_config.xml",
        "--output-path", "san_francisco_population.xml.gz",
        "--threads", context.config("processes"),
        "--config:plans.inputPlansFile", "prepared_population.xml.gz"
    ])
    assert os.path.exists("%s/san_francisco_population.xml.gz" % context.path())
    # Validate scenario
    eqasim.run(context, "org.eqasim.core.scenario.validation.RunScenarioValidator", [
        "--config-path", "san_francisco_config.xml"
    ])

    # Cleanup
    os.remove("%s/prepared_population.xml.gz" % context.path())

    return "san_francisco_config.xml"

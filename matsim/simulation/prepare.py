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
    context.config("eqasim_java_package")
    context.config("spatial_imputation_file")
    context.config("region")
    
def execute(context):
    eqasim_java_package = context.config("eqasim_java_package")
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
    
    imputation_spatial_path = "%s/Spatial/%s" % (context.config("data_path"), context.config("spatial_imputation_file"))

    eqasim.run(context, "org.eqasim.core.scenario.preparation.RunPreparation", [
        "--input-facilities-path", facilities_path,
        "--output-facilities-path", "%s_facilities.xml.gz" % eqasim_java_package,
        "--input-population-path", population_path,
        "--output-population-path", "prepared_population.xml.gz",
        "--input-network-path", network_path,
        "--output-network-path", "%s_network.xml.gz" % eqasim_java_package,
        "--threads", context.config("processes")
    ])
    if (context.config("region") == "sf"):
	    eqasim.run(context, "org.eqasim.%s.preparation.RunImputeInnerSFAttribute" % eqasim_java_package, [
	            "--sf-path", imputation_spatial_path,
	            "--input-path", "prepared_population.xml.gz",
	            "--output-path", "prepared_population.xml.gz"
	        ])
    elif (context.config("region") == "la"):
        imputation_spatial_path = "%s/Spatial/LA_area_downtowns_2227_reduced2.shp" % context.config("data_path")
        eqasim.run(context, "org.eqasim.%s.preparation.RunImputeInnerLAAttribute" % eqasim_java_package, [
	            "--la-path", imputation_spatial_path,
	            "--input-path", "prepared_population.xml.gz",
	            "--output-path", "prepared_population.xml.gz",
	            "--attribute-name", "city"
	        ])
        
        imputation_spatial_path = "%s/Spatial/Orange_County_2227.shp" % context.config("data_path")
        eqasim.run(context, "org.eqasim.%s.preparation.RunImputeInnerLAAttribute" % eqasim_java_package, [
	            "--la-path", imputation_spatial_path,
	            "--input-path", "prepared_population.xml.gz",
	            "--output-path", "prepared_population.xml.gz",
	            "--attribute-name", "orangeCounty"
	        ])
    else:
        raise Exception("This region name is not supported, Try one of the following [sf, la]")
		
        
    eqasim.run(context, "org.eqasim.%s.scenario.RunNetworkFixer" % eqasim_java_package, [
            "--input-path", "%s_network.xml.gz" % eqasim_java_package,
            "--output-path", "%s_network.xml.gz" % eqasim_java_package
        ])    

    assert os.path.exists("%s/%s_facilities.xml.gz" % (context.path(), eqasim_java_package))
    assert os.path.exists("%s/prepared_population.xml.gz" % context.path())
    assert os.path.exists("%s/%s_network.xml.gz" % (context.path(), eqasim_java_package))

    # Copy remaining input files
    households_path = "%s/%s" % (
        context.path("matsim.scenario.households"),
        context.stage("matsim.scenario.households")
    )
    shutil.copy(households_path, "%s/%s_households.xml.gz" % (context.cache_path, eqasim_java_package))

    transit_schedule_path = "%s/%s" % (
        context.path("matsim.scenario.supply.processed"),
        context.stage("matsim.scenario.supply.processed")["schedule_path"]
    )
    shutil.copy(transit_schedule_path, "%s/%s_transit_schedule.xml.gz" % (context.cache_path, eqasim_java_package))

    transit_vehicles_path = "%s/%s" % (
        context.path("matsim.scenario.supply.gtfs"),
        context.stage("matsim.scenario.supply.gtfs")["vehicles_path"]
    )
    shutil.copy(transit_vehicles_path, "%s/%s_transit_vehicles.xml.gz" % (context.cache_path, eqasim_java_package))

    # Generate base configuration
    eqasim.run(context, "org.eqasim.core.scenario.config.RunGenerateConfig", [
        "--sample-size", context.config("sampling_rate"),
        "--threads", context.config("processes"),
        "--prefix", "%s_" % eqasim_java_package,
        "--random-seed", context.config("random_seed"),
        "--output-path", "generic_config.xml"
    ])
    assert os.path.exists("%s/generic_config.xml" % context.path())

    # Adapt config
    eqasim.run(context, "org.eqasim.%s.scenario.RunAdaptConfig" % eqasim_java_package, [
        "--input-path", "generic_config.xml",
        "--output-path", "%s_config.xml" % eqasim_java_package
    ])
    assert os.path.exists("%s/%s_config.xml" % (context.path(), eqasim_java_package))

    # Route population
    eqasim.run(context, "org.eqasim.core.scenario.routing.RunPopulationRouting", [
        "--config-path", "%s_config.xml" % eqasim_java_package,
        "--output-path", "%s_population.xml.gz" % eqasim_java_package,
        "--threads", context.config("processes"),
        "--config:plans.inputPlansFile", "prepared_population.xml.gz"
    ])
    assert os.path.exists("%s/%s_population.xml.gz" % (context.path(), eqasim_java_package))
    # Validate scenario
    eqasim.run(context, "org.eqasim.core.scenario.validation.RunScenarioValidator", [
        "--config-path", "%s_config.xml" % eqasim_java_package
    ])

    # Cleanup
    os.remove("%s/prepared_population.xml.gz" % context.path())

    return "%s_config.xml" % eqasim_java_package

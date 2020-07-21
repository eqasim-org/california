import shutil

def configure(context):
    context.stage("matsim.simulation.run")
    context.stage("matsim.simulation.prepare")
    context.stage("matsim.runtime.eqasim")

    context.config("output_path")

def execute(context):
    config_path = "%s/%s" % (
        context.path("matsim.simulation.prepare"),
        context.stage("matsim.simulation.prepare")
    )

    for name in [
        "san_francisco_households.xml.gz",
        "san_francisco_population.xml.gz",
        "san_francisco_facilities.xml.gz",
        "san_francisco_network.xml.gz",
        "san_francisco_transit_schedule.xml.gz",
        "san_francisco_transit_vehicles.xml.gz",
        "san_francisco_config.xml"
    ]:
        shutil.copy(
            "%s/%s" % (context.path("matsim.simulation.prepare"), name),
            "%s/%s" % (context.config("output_path"), name)
        )

    shutil.copy(
        "%s/%s" % (context.path("matsim.runtime.eqasim"), context.stage("matsim.runtime.eqasim")),
        context.config("output_path")
    )

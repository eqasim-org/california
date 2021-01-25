import os.path
from gtfsmerger import GTFSMerger
import matsim.runtime.pt2matsim as pt2matsim
import zipfile
import pandas as pd

def configure(context):
    context.stage("matsim.runtime.java")
    context.stage("matsim.runtime.pt2matsim")
    context.stage("matsim.scenario.supply.gtfs_merger")
    context.config("data_path")

def execute(context):
    
    pt2matsim.run(context, "org.matsim.pt2matsim.run.Gtfs2TransitSchedule", [
        "%s/gtfs/gtfs_merged" % context.config("data_path"),
        "dayWithMostServices", "EPSG:2227", # TODO: dayWithMostServices should be made explicit and configurable!
        "%s/transit_schedule.xml.gz" % context.path(),
        "%s/transit_vehicles.xml.gz" % context.path()
    ])

    assert(os.path.exists("%s/transit_schedule.xml.gz" % context.path()))
    assert(os.path.exists("%s/transit_vehicles.xml.gz" % context.path()))

    return dict(
        schedule_path = "transit_schedule.xml.gz",
        vehicles_path = "transit_vehicles.xml.gz"
    )

import os.path

import matsim.runtime.pt2matsim as pt2matsim

def configure(context):
    context.stage("matsim.runtime.java")
    context.stage("matsim.runtime.pt2matsim")
    context.config("osm_file_pt2matsim")
    context.config("data_path")

def execute(context):
    content = """<?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE config SYSTEM "http://www.matsim.org/files/dtd/config_v2.dtd">
    <config>
	<module name="OsmConverter" >
		<!-- Sets whether the detailed geometry of the roads should be retained in the conversion or not.
		Keeping the detailed paths results in a much higher number of nodes and links in the resulting MATSim network.
		Not keeping the detailed paths removes all nodes where only one road passes through, thus only real intersections
		or branchings are kept as nodes. This reduces the number of nodes and links in the network, but can in some rare
		cases generate extremely long links (e.g. for motorways with only a few ramps every few kilometers).
		Defaults to <code>false</code>. -->
		<param name="keepPaths" value="false" />
		<!-- If true: The osm tags for ways and containing relations are saved as link attributes in the network. Increases filesize. Default: true. -->
		<param name="keepTagsAsAttributes" value="true" />
		<!-- Keep all ways (highway=* and railway=*) with public transit even if they don't have wayDefaultParams defined -->
		<param name="keepWaysWithPublicTransit" value="true" />
		<param name="maxLinkLength" value="1500.0" />
		<!-- The path to the osm file. -->
		<param name="osmFile" value="" />
		<param name="outputCoordinateSystem" value="EPSG:2227" />
		<param name="outputNetworkFile" value="network.xml.gz" />
		<!-- In case the speed limit allowed does not represent the speed a vehicle can actually realize, e.g. by constrains of
		traffic lights not explicitly modeled, a kind of "average simulated speed" can be used.
		Defaults to false. Set true to scale the speed limit down by the value specified by the wayDefaultParams) -->
		<param name="scaleMaxSpeed" value="false" />
		<parameterset type="routableSubnetwork" >
			<param name="allowedTransportModes" value="car" />
			<param name="subnetworkMode" value="car" />
		</parameterset>
		<parameterset type="routableSubnetwork" >
			<param name="allowedTransportModes" value="car" />
			<param name="subnetworkMode" value="car_passenger" />
		</parameterset>
		<parameterset type="routableSubnetwork" >
			<param name="allowedTransportModes" value="car" />
			<param name="subnetworkMode" value="taxi" />
		</parameterset>
		<parameterset type="routableSubnetwork" >
			<param name="allowedTransportModes" value="bus" />
			<param name="subnetworkMode" value="bus" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="car" />
			<param name="freespeed" value="95.3333" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="1800.0" />
			<param name="lanes" value="3.0" />
			<param name="oneway" value="true" />
			<param name="osmKey" value="highway" />
			<param name="osmValue" value="motorway" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="car" />
			<param name="freespeed" value="51.55" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="1500.0" />
			<param name="lanes" value="1.0" />
			<param name="oneway" value="true" />
			<param name="osmKey" value="highway" />
			<param name="osmValue" value="motorway_link" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="car" />
			<param name="freespeed" value="80" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="2000.0" />
			<param name="lanes" value="2.0" />
			<param name="oneway" value="false" />
			<param name="osmKey" value="highway" />
			<param name="osmValue" value="trunk" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="car" />
			<param name="freespeed" value="51" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="1500.0" />
			<param name="lanes" value="1.0" />
			<param name="oneway" value="false" />
			<param name="osmKey" value="highway" />
			<param name="osmValue" value="trunk_link" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="car" />
			<param name="freespeed" value="73.5555" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="1500.0" />
			<param name="lanes" value="2.0" />
			<param name="oneway" value="false" />
			<param name="osmKey" value="highway" />
			<param name="osmValue" value="primary" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="car" />
			<param name="freespeed" value="51" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="1500.0" />
			<param name="lanes" value="1.0" />
			<param name="oneway" value="false" />
			<param name="osmKey" value="highway" />
			<param name="osmValue" value="primary_link" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="car" />
			<param name="freespeed" value="51" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="1100.0" />
			<param name="lanes" value="2.0" />
			<param name="oneway" value="false" />
			<param name="osmKey" value="highway" />
			<param name="osmValue" value="secondary" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="car" />
			<param name="freespeed" value="51" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="1100.0" />
			<param name="lanes" value="1.0" />
			<param name="oneway" value="false" />
			<param name="osmKey" value="highway" />
			<param name="osmValue" value="secondary_link" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="car" />
			<param name="freespeed" value="44" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="800.0" />
			<param name="lanes" value="1.0" />
			<param name="oneway" value="false" />
			<param name="osmKey" value="highway" />
			<param name="osmValue" value="tertiary" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="car" />
			<param name="freespeed" value="44" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="800.0" />
			<param name="lanes" value="1.0" />
			<param name="oneway" value="false" />
			<param name="osmKey" value="highway" />
			<param name="osmValue" value="tertiary_link" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="car" />
			<param name="freespeed" value="36" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="600.0" />
			<param name="lanes" value="1.0" />
			<param name="oneway" value="false" />
			<param name="osmKey" value="highway" />
			<param name="osmValue" value="minor" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="car" />
			<param name="freespeed" value="36" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="600.0" />
			<param name="lanes" value="1.0" />
			<param name="oneway" value="false" />
			<param name="osmKey" value="highway" />
			<param name="osmValue" value="unclassified" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="car" />
			<param name="freespeed" value="36" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="600.0" />
			<param name="lanes" value="1.0" />
			<param name="oneway" value="false" />
			<param name="osmKey" value="highway" />
			<param name="osmValue" value="residential" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="car" />
			<param name="freespeed" value="29" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="600.0" />
			<param name="lanes" value="1.0" />
			<param name="oneway" value="false" />
			<param name="osmKey" value="highway" />
			<param name="osmValue" value="living_street" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="rail" />
			<param name="freespeed" value="127" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="9999.0" />
			<param name="lanes" value="1.0" />
			<param name="oneway" value="false" />
			<param name="osmKey" value="railway" />
			<param name="osmValue" value="rail" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="rail" />
			<param name="freespeed" value="30" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="9999.0" />
			<param name="lanes" value="1.0" />
			<param name="oneway" value="true" />
			<param name="osmKey" value="railway" />
			<param name="osmValue" value="tram" />
		</parameterset>
		<parameterset type="wayDefaultParams" >
			<param name="allowedTransportModes" value="rail" />
			<param name="freespeed" value="60" />
			<param name="freespeedFactor" value="1.0" />
			<param name="laneCapacity" value="9999.0" />
			<param name="lanes" value="1.0" />
			<param name="oneway" value="false" />
			<param name="osmKey" value="railway" />
			<param name="osmValue" value="light_rail" />
		</parameterset>
	</module>

    </config>
    """
    pt2matsim.run(context, "org.matsim.pt2matsim.run.CreateDefaultOsmConfig", [
        "config_template.xml"
    ])
    
    content = content.replace(
            '<param name="osmFile" value="" />',
            '<param name="osmFile" value="%s/osm/%s" />' % (context.config("data_path"), context.config("osm_file_pt2matsim")))

    with open("%s/config.xml" % context.path(), "w+") as f_write:
        f_write.write(content)

    pt2matsim.run(context, "org.matsim.pt2matsim.run.Osm2MultimodalNetwork", [
        "config.xml"
    ])

    assert(os.path.exists("%s/network.xml.gz" % context.path()))
    return "network.xml.gz"

def validate(context):
    if not os.path.exists("%s/osm/%s" % (context.config("data_path"), context.config("osm_file_pt2matsim"))):
        raise RuntimeError("OSM data is not available")

    return os.path.getsize("%s/osm/%s" % (context.config("data_path"), context.config("osm_file_pt2matsim")))

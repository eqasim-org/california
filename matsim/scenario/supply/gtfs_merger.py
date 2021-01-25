import os.path
from gtfsmerger import GTFSMerger
import matsim.runtime.pt2matsim as pt2matsim
import zipfile
import pandas as pd

def configure(context):
    context.config("data_path")
    context.config("region")

def execute(context):
    n = GTFSMerger()
    if (context.config("region") == "sf"):
        f_1 = open("%s/gtfs/AC Transit.zip" % context.config("data_path"), 'rb').read()
        f_2 = open("%s/gtfs/Altamont Corridor Express.zip" % context.config("data_path"), 'rb').read()
        f_3 = open("%s/gtfs/BART.zip" % context.config("data_path"), 'rb').read()
        f_4 = open("%s/gtfs/CaptiolCorridor.zip" % context.config("data_path"), 'rb').read()
        f_5 = open("%s/gtfs/County Connection.zip" % context.config("data_path"), 'rb').read()
        f_6 = open("%s/gtfs/CT-GTFS.zip" % context.config("data_path"), 'rb').read()
        f_7 = open("%s/gtfs/Dumbarton Express.zip" % context.config("data_path"), 'rb').read()
        f_8 = open("%s/gtfs/Emery Go-Round.zip" % context.config("data_path"), 'rb').read()
        f_9 = open("%s/gtfs/FAST Transit.zip" % context.config("data_path"), 'rb').read()
        f_10 = open("%s/gtfs/Golde Gate Transit.zip" % context.config("data_path"), 'rb').read()
        f_11 = open("%s/gtfs/Livermore.zip" % context.config("data_path"), 'rb').read()
        f_12 = open("%s/gtfs/Marin Transit.zip" % context.config("data_path"), 'rb').read()
        f_13 = open("%s/gtfs/mountainview.zip" % context.config("data_path"), 'rb').read()
        f_14 = open("%s/gtfs/Petaluma Transit.zip" % context.config("data_path"), 'rb').read()
        f_15 = open("%s/gtfs/pinole.zip" % context.config("data_path"), 'rb').read()
        f_16 = open("%s/gtfs/Rio Vista Delta Breeze.zip" % context.config("data_path"), 'rb').read()
        f_17 = open("%s/gtfs/samtrans.zip" % context.config("data_path"), 'rb').read()
        f_18 = open("%s/gtfs/SantaRosa.zip" % context.config("data_path"), 'rb').read()
        f_19 = open("%s/gtfs/SF Bay Ferries.zip" % context.config("data_path"), 'rb').read()
        f_20 = open("%s/gtfs/sf_missionbayTMA.zip" % context.config("data_path"), 'rb').read()
        f_21 = open("%s/gtfs/sf_watertaxi.zip" % context.config("data_path"), 'rb').read()
        f_22 = open("%s/gtfs/SFMTA.zip" % context.config("data_path"), 'rb').read()
        f_23 = open("%s/gtfs/SMART.zip" % context.config("data_path"), 'rb').read()
        f_24 = open("%s/gtfs/soltrans.zip" % context.config("data_path"), 'rb').read()
        f_25 = open("%s/gtfs/sonoma.zip" % context.config("data_path"), 'rb').read()
        f_26 = open("%s/gtfs/stanford.zip" % context.config("data_path"), 'rb').read()
        f_27 = open("%s/gtfs/Vacaville.zip" % context.config("data_path"), 'rb').read()
        f_28 = open("%s/gtfs/vinetransit-ca-us.zip" % context.config("data_path"), 'rb').read()
        f_29 = open("%s/gtfs/VTA.zip" % context.config("data_path"), 'rb').read()
        f_30 = open("%s/gtfs/sonoma_marin.zip" % context.config("data_path"), 'rb').read()
        f_31 = open("%s/gtfs/concord.zip" % context.config("data_path"), 'rb').read()
        f_32 = open("%s/gtfs/cityofpaloalto_google_transit.zip" % context.config("data_path"), 'rb').read()
        f_33 = open("%s/gtfs/commute-ca-us.zip" % context.config("data_path"), 'rb').read()
        f_34 = open("%s/gtfs/commuteorg-shuttle_20160202_1748.zip" % context.config("data_path"), 'rb').read()
        f_35 = open("%s/gtfs/stanford_shuttle.zip" % context.config("data_path"), 'rb').read()

        n.merge_from_bytes_list([f_1, f_2, f_3, f_4, f_5, f_6, f_7, f_8, f_9, f_10, f_11, f_12, f_13, f_14, f_15, f_16, f_17, f_18, f_19, f_20, f_21, f_22, f_23, f_24, f_25, f_26,
        f_27, f_28, f_29, f_30, f_31, f_32, f_33, f_34, f_35 ])
    elif (context.config("region") == "la"):
        f_1 = open("%s/gtfs/anaheim-ca-us.zip" % context.config("data_path"), 'rb').read()
        f_2 = open("%s/gtfs/corona.zip" % context.config("data_path"), 'rb').read()
        f_3 = open("%s/gtfs/culver.zip" % context.config("data_path"), 'rb').read()
        f_4 = open("%s/gtfs/duarte.zip" % context.config("data_path"), 'rb').read()

        f_5 = open("%s/gtfs/EImonte.zip" % context.config("data_path"), 'rb').read()
        f_6 = open("%s/gtfs/foothill.zip" % context.config("data_path"), 'rb').read()
        f_7 = open("%s/gtfs/gtfs_metrolink_trains.zip" % context.config("data_path"), 'rb').read()
        # la_ladot has an issue with trip 1- 16754 cannot find it in stop times
        #there was a space before one of the departures of trips 16754, the sapce is now removed
        f_8 = open("%s/gtfs/la_ladot.zip" % context.config("data_path"), 'rb').read()

        f_9 = open("%s/gtfs/la_metro_bus.zip" % context.config("data_path"), 'rb').read()
        f_10 = open("%s/gtfs/LA Metro.zip" % context.config("data_path"), 'rb').read()
        f_11 = open("%s/gtfs/la_metrolink.zip" % context.config("data_path"), 'rb').read()
        f_12 = open("%s/gtfs/longbeach.zip" % context.config("data_path"), 'rb').read()

        f_13 = open("%s/gtfs/monterey.zip" % context.config("data_path"), 'rb').read()
        f_14 = open("%s/gtfs/norwalk.zip" % context.config("data_path"), 'rb').read()
        f_15 = open("%s/gtfs/orange_new.zip" % context.config("data_path"), 'rb').read()
        f_16 = open("%s/gtfs/oxnard.zip" % context.config("data_path"), 'rb').read()

        f_17 = open("%s/gtfs/pasendane.zip" % context.config("data_path"), 'rb').read()
        f_18 = open("%s/gtfs/penisula.zip" % context.config("data_path"), 'rb').read()
        f_19 = open("%s/gtfs/riverside.zip" % context.config("data_path"), 'rb').read()
        f_20 = open("%s/gtfs/VictorValley.zip" % context.config("data_path"), 'rb').read()
        ##removed transfers.txt file from the santa_monica_bus.zip as it did not have any transfer times
        f_21 = open("%s/gtfs/santa_monica_bus.zip" % context.config("data_path"), 'rb').read()
        
        f_22 = open("%s/gtfs/sb.zip" % context.config("data_path"), 'rb').read()
        f_23 = open("%s/gtfs/simi.zip" % context.config("data_path"), 'rb').read()
        f_24 = open("%s/gtfs/thousandsoaks.zip" % context.config("data_path"), 'rb').read()
        
        #Route 0-SS not found in routes.txt for torrance.zip
        #removed SS route from trips file
        #removed all shoppers special stop times as those trips do not exist
        f_25 = open("%s/gtfs/torrance.zip" % context.config("data_path"), 'rb').read()
        f_26 = open("%s/gtfs/ventura.zip" % context.config("data_path"), 'rb').read()
        n.merge_from_bytes_list([f_1, f_2, f_3, f_4, f_5, f_6, f_7, f_8, f_9, f_10, f_11, f_12, f_13, f_14, f_15, f_16, f_17, f_18, f_19, f_20, f_21, f_22, f_23, f_24, f_25, f_26])      

    n.get_zipfile("%s/gtfs/gtfs_merged.zip" % context.config("data_path"))
    with zipfile.ZipFile("%s/gtfs/gtfs_merged.zip" % context.config("data_path"), 'r') as zip_ref:
        zip_ref.extractall("%s/gtfs/gtfs_merged/" % context.config("data_path"))
        
    data = pd.read_csv("%s/gtfs/gtfs_merged/calendar.txt" % context.config("data_path"))
    data["end_date"] = data["end_date"].str.replace("-","")
    data["start_date"] = data["start_date"].str.replace("-","")
    data.to_csv("%s/gtfs/gtfs_merged/calendar.txt" % context.config("data_path"))

    data = pd.read_csv("%s/gtfs/gtfs_merged/calendar_dates.txt" % context.config("data_path"))
    data["date"] = data["date"].str.replace("-","")
    data.to_csv("%s/gtfs/gtfs_merged/calendar_dates.txt" % context.config("data_path"))

    data = pd.read_csv("%s/gtfs/gtfs_merged/transfers.txt" % context.config("data_path"))
    data["min_transfer_time"] = data["min_transfer_time"].astype(str).str.replace(".0","")
    data.to_csv("%s/gtfs/gtfs_merged/transfers.txt" % context.config("data_path"))
        

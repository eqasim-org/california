import shutil
import os.path

import matsim.runtime.eqasim as eqasim

def configure(context):
    context.stage("matsim.simulation.prepare")

    context.stage("matsim.runtime.java")
    context.stage("matsim.runtime.eqasim")
    context.config("eqasim_java_package")

def execute(context):
    config_path = "%s/%s" % (
        context.path("matsim.simulation.prepare"),
        context.stage("matsim.simulation.prepare")
    )

    # Run simulation
    #eqasim.run(context, "org.eqasim.san_francisco.RunSimulation", [
    #    "--config-path", config_path,
    #    "--config:controler.lastIteration", str(30),
    #    "--config:controler.writeEventsInterval", str(10),
    #    "--config:controler.writePlansInterval", str(10),
    #    "--config:planscalcroute.teleportedModeParameters[mode=walk].beelineDistanceFactor", str(1.35),
    #    "--config:planscalcroute.teleportedModeParameters[mode=bike].teleportedModeSpeed", str(9.1),
    #    "--config:planscalcroute.teleportedModeParameters[mode=walk].teleportedModeSpeed", str(4.0),
    #    "--config:transitRouter.searchRadius", str(9000.0),
    #    "--config:transitRouter.maxBeelineWalkConnectionDistance", str(4500.0),
    #    "--mode-parameter:walk.alpha_u", str(0.1),
    #    "--mode-parameter:bike.alpha_u", str(-1.2),
    #    "--config:eqasim.crossingPenalty", str(7.5),
    #    "--config:strategy.strategysettings[strategyName=DiscreteModeChoice].weight", str(0.1),
    #    "--config:strategy.strategysettings[strategyName=KeepLastSelected].weight", str(0.9)
    #
    #])
    
    eqasim.run(context, "org.eqasim.%s.RunSimulation" % context.config("eqasim_java_package"), [
        "--config-path", config_path,
        "--config:controler.lastIteration", str(30),
        "--config:controler.writeEventsInterval", str(10),
        "--config:controler.writePlansInterval", str(10),
        "--config:planscalcroute.teleportedModeParameters[mode=walk].beelineDistanceFactor", str(1.35),
        "--config:planscalcroute.teleportedModeParameters[mode=bike].teleportedModeSpeed", str(9.1),
        "--config:planscalcroute.teleportedModeParameters[mode=walk].teleportedModeSpeed", str(4.0),
        "--config:transitRouter.searchRadius", str(9000.0),
        "--config:transitRouter.maxBeelineWalkConnectionDistance", str(4500.0),
        "--config:eqasim.crossingPenalty", str(7.5),
        "--config:strategy.strategysettings[strategyName=DiscreteModeChoice].weight", str(0.1),
        "--config:strategy.strategysettings[strategyName=KeepLastSelected].weight", str(0.9)
    
    ])
    
    
    assert os.path.exists("%s/simulation_output/output_events.xml.gz" % context.path())

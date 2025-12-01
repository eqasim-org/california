import subprocess as sp
import os, os.path

import matsim.runtime.git as git
import matsim.runtime.java as java
import matsim.runtime.maven as maven

def configure(context):
    context.stage("matsim.runtime.git")
    context.stage("matsim.runtime.java")
    context.stage("matsim.runtime.maven")
    context.config("eqasim_java_package")

    context.config("eqasim_version")

def run(context, command, arguments):
    version = context.config("eqasim_version")
    eqasim_java_package = context.config("eqasim_java_package")
    # Make sure there is a dependency
    context.stage("matsim.runtime.eqasim")

    jar_path = "%s/eqasim-java/%s/target/%s-%s.jar" % (
        context.path("matsim.runtime.eqasim"), eqasim_java_package, eqasim_java_package, version
    )
    java.run(context, command, arguments, jar_path)

def execute(context):
    version = context.config("eqasim_version")
    eqasim_java_package = context.config("eqasim_java_package")

    # Clone repository and checkout tag version
    REPO_DIR = "eqasim-java"
    git.run(context, [
        "clone", "https://github.com/eqasim-org/eqasim-java.git",
        "--branch", "develop",
        "--single-branch", f"{context.path()}/{REPO_DIR}",
        "--depth", "1"
    ])
    git.run(context, [
        "fetch", "--tags"
    ], cwd=f"{context.path()}/{REPO_DIR}")
    git.run(context, [
        "checkout", f"v{version}"
    ], cwd=f"{context.path()}/{REPO_DIR}")

    # Build eqasim
    maven.run(context, ["-Pstandalone", "--projects", "san_francisco", "--also-make", "package", "-DskipTests=true"], cwd = "%s/eqasim-java" % context.path())
    jar_path = "%s/eqasim-java/%s/target/%s-%s.jar" % (context.path(), eqasim_java_package, eqasim_java_package, version)

    return "eqasim-java/san_francisco/target/san_francisco-%s.jar" % version

# Apache Spark with Scala
Based on the material by Frank Kane, Sundog Education.

### Directory structure
```
.
├── LICENSE
├── README.md
├── build.sbt
├── project
├── res
├── src
└── target
```

- **build.sbt** - The primary build definition file for SBT (Scala Build Tool). It specifies project dependencies, settings, and build instructions.
- `project/` - Contains SBT project configuration files and subfolders used for managing the build process.
  - build.properties - defines the SBT version to use for building the project.
- `res/` - A resource directory containing datasets and text files used as input or examples for Spark jobs and exercises.
- `scr/` - Source code directory.
  - `main/` - the main application source code.
  - `scala-refresher/` -  Scala refresher scripts or exercises.

### Setting up local environment
1. Verify that IntelliJ and Java 11 are installed.
2. Configure the project SDK in IntelliJ to use jdk-11.
3. Install the Scala plugin for IntelliJ.
2. Validate the `build.sbt`.
3. Mark `scr/main` as root directory.

# About Spark

**Spark** is a fast and general engine for large data processing. It parallelizes processing and is highly scalable.

It is faster than map reduce (Hadoop) due to the DAG engine (directed acyclic graph). It optimizes the workflow and is memory-based.

### Spark architecture

### Components of Spark

Spark can be written in Python, Java, and Scala.

Benefits of using Scala:
- Spark itself is written in Scala.
- Scala's functional programming model is a good fit for distributed processing.
- It gives fast performance (Scala compiles to Java bytecode).
- Less code boilerplate than Java.
- Faster than Python.

## Spark RDDs

## Spark SQL




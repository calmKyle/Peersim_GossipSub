#!/bin/bash

# Build LIB_JARS dynamically
LIB_JARS=$(find -L lib/ -name "*.jar" | tr [:space:] :)

# Step 1: Compile
echo "Compiling Java sources..."
mkdir -p classes
javac -sourcepath src -classpath "$LIB_JARS" -d classes $(find -L src/ -name "*.java")

# Check if compilation succeeded
if [ $? -ne 0 ]; then
    echo "Compilation failed. Exiting."
    exit 1
fi

# Step 2: Run simulations
JAVA_OPTS="-Xmx32000m -Xms2000m"
CLASSPATH="${LIB_JARS}:classes"
CONFIG_DIR="MaliciousConfig"

# Loop through each .cfg file
for cfg_path in "$CONFIG_DIR"/*.cfg; do
    # Extract just the file name (e.g., MaliciousGossipConfig_10.cfg)
    cfg_file=$(basename "$cfg_path")

    echo "========================================"
    echo "Running simulation with: $cfg_file"
    start_time=$(date +%s)

    # Run the simulation
    java $JAVA_OPTS -cp "$CLASSPATH" peersim.Simulator "$cfg_path"

    end_time=$(date +%s)
    echo "Finished $cfg_file in $((end_time - start_time)) seconds"
    echo "========================================"
done

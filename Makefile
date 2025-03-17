.PHONY: all clean doc compile

LIB_JARS=`find -L lib/ -name "*.jar" | tr [:space:] :`

compile:
	mkdir -p classes
	javac -sourcepath src -classpath $(LIB_JARS) -d classes `find -L -name "*.java"`

doc:
	mkdir -p doc
	javadoc -sourcepath src -classpath $(LIB_JARS) -d doc peersim.kademlia

run:
	java -Xmx500m -cp $(LIB_JARS):classes peersim.Simulator example.cfg

rungossip:
	java -Xmx5000m -cp $(LIB_JARS):classes peersim.Simulator gossipConfig.cfg

PANDAS_Gossip:
# 	java -Xmx48000m -Xms2000m -cp  $(LIB_JARS):classes peersim.Simulator GossipConfig.cfg
    java -agentpath:/home/calmk/.local/share/JetBrains/IntelliJIdea2024.3/JProfiler/libjprofilerti.so=port=8849,nowait -Xmx48000m -Xms2000m -cp $(LIB_JARS):classes peersim.Simulator GossipConfig.cfg

all: compile doc run

clean:
	rm -fr classes doc

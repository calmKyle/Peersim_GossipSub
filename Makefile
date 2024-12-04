.PHONY: all clean doc compile

LIB_JARS=$(shell powershell -Command "Get-ChildItem -Recurse -Filter *.jar -Path lib | ForEach-Object { $_.FullName } | ForEach-Object { '\"' + $_.Replace('\\', '/') + '\"' } -join ';'")

compile:
	mkdir -p classes
	javac -sourcepath src -classpath $(LIB_JARS) -d classes $(shell dir /s /b src\*.java)

doc:
	mkdir -p doc
	javadoc -sourcepath src -classpath $(LIB_JARS) -d doc peersim.kademlia

run:
	java -Xmx500m -cp $(LIB_JARS);classes peersim.Simulator example.cfg

rungossip:
	java -Xmx500m -cp $(LIB_JARS);classes peersim.Simulator gossipConfig.cfg

all: compile doc run

clean:
	rm -rf classes doc

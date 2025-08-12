@echo off
setlocal enabledelayedexpansion

rem ===============================================
rem =============== Collect LIB JARS ==============
rem ===============================================
set "LIB_JARS="
for %%f in (lib\*.jar) do (
    if "!LIB_JARS!"=="" (
        set "LIB_JARS=%%f"
    ) else (
        set "LIB_JARS=!LIB_JARS!;%%f"
    )
)

rem ===============================================
rem ============= Show usage if needed ============
rem ===============================================
if "%1"=="" goto usage

if /i "%1"=="compile"       goto compile
if /i "%1"=="doc"           goto doc
if /i "%1"=="run"           goto run
if /i "%1"=="rungossip"     goto rungossip
if /i "%1"=="PANDAS_Gossip" goto PANDAS_Gossip
if /i "%1"=="all"           goto all
if /i "%1"=="clean"         goto clean

:usage
echo.
echo Usage: build.bat [compile ^| doc ^| run ^| rungossip ^| PANDAS_Gossip ^| all ^| clean]
echo.
goto :end


rem ===============================================
rem ================ compile ======================
rem ===============================================
:compile
echo [compile] Creating classes folder ...
mkdir classes 2>nul

echo [compile] Compiling Java sources ...
for /R src %%f in (*.java) do (
    javac -sourcepath src -classpath "%LIB_JARS%" -d classes "%%f"
)

echo [compile] Done.
goto :end


rem ===============================================
rem ================== doc ========================
rem ===============================================
:doc
echo [doc] Creating doc folder ...
mkdir doc 2>nul

echo [doc] Generating javadoc for package peersim.kademlia ...
javadoc -sourcepath src -classpath "%LIB_JARS%" -d doc peersim.kademlia

echo [doc] Done.
goto :end


rem ===============================================
rem ================== run ========================
rem ===============================================
:run
echo [run] Running simulator with example.cfg ...
java -Xmx500m -cp "%LIB_JARS%;classes" peersim.Simulator example.cfg

echo [run] Done.
goto :end


rem ===============================================
rem =============== rungossip =====================
rem ===============================================
:rungossip
echo [rungossip] Running simulator with gossipConfig.cfg ...
java -Xmx5000m -cp "%LIB_JARS%;classes" peersim.Simulator gossipConfig.cfg

echo [rungossip] Done.
goto :end


rem ===============================================
rem ============= PANDAS_Gossip ===================
rem ===============================================
:PANDAS_Gossip
echo [PANDAS_Gossip] Running simulator with GossipConfig.cfg ...
java -Xmx32000m -Xms2000m -cp "%LIB_JARS%;classes" peersim.Simulator GossipConfig.cfg

echo [PANDAS_Gossip] Done.
goto :end


rem ===============================================
rem ================= all =========================
rem ===============================================
:all
echo [all] compile, doc, run
call "%~f0" compile
call "%~f0" doc
call "%~f0" run
goto :end


rem ===============================================
rem ================= clean =======================
rem ===============================================
:clean
echo [clean] Removing classes and doc folders ...
rmdir /S /Q classes 2>nul
rmdir /S /Q doc 2>nul

echo [clean] Done.
goto :end

:end
endlocal


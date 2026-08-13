@echo off
setlocal

rem RocketMQ binary, Broker configuration, and JDK locations.
set "ROCKETMQ_HOME=%~dp0runtime\rocketmq-all-5.3.2-bin-release"
set "BROKER_CONFIG=%~dp0conf\broker.conf"
set "JAVA_HOME=C:\Program Files\Java\jdk-17.0.18"

rem Use a 512 MB heap and 512 MB direct-memory limit for local development.
rem The distribution defaults are intended for a dedicated server.
set "JAVA_OPT_EXT=-Xms512m -Xmx512m -XX:MaxDirectMemorySize=512m -Duser.home=%~dp0data"

if not exist "%ROCKETMQ_HOME%\bin\mqbroker.cmd" (
    echo [ERROR] RocketMQ runtime not found: %ROCKETMQ_HOME%
    exit /b 1
)

if not exist "%BROKER_CONFIG%" (
    echo [ERROR] Broker config not found: %BROKER_CONFIG%
    exit /b 1
)

echo [INFO] Starting RocketMQ Broker on 127.0.0.1:10911 ...
call "%ROCKETMQ_HOME%\bin\mqbroker.cmd" -c "%BROKER_CONFIG%"

endlocal

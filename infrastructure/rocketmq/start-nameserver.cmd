@echo off
setlocal

rem RocketMQ binary location. %~dp0 is the directory containing this script.
set "ROCKETMQ_HOME=%~dp0runtime\rocketmq-all-5.3.2-bin-release"

rem JDK 17 verified for this learning environment. RocketMQ requires JAVA_HOME.
set "JAVA_HOME=C:\Program Files\Java\jdk-17.0.18"

rem Override the server-oriented 2 GB defaults for local development.
rem user.home keeps RocketMQ logs inside this project.
set "JAVA_OPT_EXT=-Xms256m -Xmx256m -Xmn128m -Duser.home=%~dp0data"

if not exist "%ROCKETMQ_HOME%\bin\mqnamesrv.cmd" (
    echo [ERROR] RocketMQ runtime not found: %ROCKETMQ_HOME%
    exit /b 1
)

echo [INFO] Starting RocketMQ NameServer on 127.0.0.1:9876 ...
call "%ROCKETMQ_HOME%\bin\mqnamesrv.cmd"

endlocal

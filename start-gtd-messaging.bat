
 @echo off
title GoTrustDeal message Service (local)

echo ============================================
echo   GoTrustDeal message  Service - LOCAL
echo ============================================
echo.

java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8002 -jar "messaging-service/target/messaging-service-1.0.0-SNAPSHOT.jar" ^
  --spring.profiles.active=local>>local.log

if errorlevel 1 (
    echo.
    echo ============================================
    echo   message Service failed to start.
    echo ============================================
)

pause 
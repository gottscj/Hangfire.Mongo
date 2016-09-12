@echo off
cd "%~dp0"
.nuget\NuGet.exe install .nuget\packages.config -OutputDirectory packages
packages\psake.4.6.0\tools\psake.cmd .\build.ps1 %*
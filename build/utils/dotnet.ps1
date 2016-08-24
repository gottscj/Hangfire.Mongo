function Clean-Project-Dotnet {
	<#
	.SYNOPSIS
	Cleans project.json output
	#>
	[CmdletBinding()]
	param(
		[Parameter(Mandatory=$True, ValueFromPipeline=$True)]
		[System.IO.FileInfo]$project,
		[Parameter()]
		[string]$config,
		[Parameter()]
		[string[]]$frameworks,
		[Parameter()]
		[string]$verbosity
	)
	PROCESS {
	
		$projectJson = Get-Content $project.FullName;
	
		[System.Reflection.Assembly]::LoadWithPartialName("System.Web.Extensions") | Out-Null;
		$serializer = New-Object System.Web.Script.Serialization.JavaScriptSerializer;
		
		$projectJson = $serializer.DeserializeObject($projectJson);
		
		$cleanFrameworks = $frameworks;
		
		if (!$cleanFrameworks) {
			# No frameworks specified, build for all
			$cleanFrameworks = $projectJson.frameworks.Keys;
		}
		
		foreach ($framework in $cleanFrameworks) {
			Get-ChildItem $project.DirectoryName -Include bin,obj -Recurse |
				Get-ChildItem -Filter "$config\$framework" -ErrorAction SilentlyContinue | 
					Remove-Item -Recurse -Force;
		}
	}
}

function Build-Project-Dotnet {
	<#
	.SYNOPSIS
	Builds project.json with dotnet.exe
	#>
	[CmdletBinding()]
	param(
		[Parameter(Mandatory=$True, ValueFromPipeline=$True)]
		[System.IO.FileInfo]$project,
		[Parameter()]
		[string]$config,
		[Parameter()]
		[string[]]$frameworks,
		[Parameter()]
		[string]$outputPath,
		[Parameter()]
		[string]$verbosity
	)
	PROCESS {
		
		$projectJson = Get-Content $project.FullName;
	
		[System.Reflection.Assembly]::LoadWithPartialName("System.Web.Extensions") | Out-Null;
		$serializer = New-Object System.Web.Script.Serialization.JavaScriptSerializer;
		
		$projectJson = $serializer.DeserializeObject($projectJson);
		
		$buildFrameworks = $frameworks;
		
		if (!$buildFrameworks) {
			# No frameworks specified, build for all
			$buildFrameworks = $projectJson.frameworks.Keys;
		}
		
		exec {
			dotnet restore $project.DirectoryName
		}
		
		foreach ($framework in $buildFrameworks) {
		
			# Try framework-level buildOptions.outputName:
			$binaryName = $projectJson.frameworks[$framework].buildOptions.outputName;
				
			if (!$binaryName) {
				# Try project-level buildOptions.outputName:
				$binaryName = $projectJson.buildOptions.outputName;
			}
			
			if (!$binaryName) {
				# Fallback to project directory name
				$binaryName = $project.Directory.Name;
			}
			
			exec {
				dotnet build $project.DirectoryName --configuration $config --framework $framework --output "$outputPath\$framework\$binaryName"
			}
		}

	}
}
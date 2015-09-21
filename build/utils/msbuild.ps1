function Clean-Project {
	<#
	.SYNOPSIS
	Cleans project by using MSBuild
	#>
	[CmdletBinding()]
	param(
		[Parameter(Mandatory=$True, ValueFromPipeline=$True)]
		[System.IO.FileInfo]$project,
		[Parameter()]
		[string]$config,
		[Parameter()]
		[string]$platform,
		[Parameter()]
		[string]$verbosity
	)
	PROCESS {

		exec {
			msbuild $project.fullname /t:Clean /p:Confifuration=$config /p:Platform=$platform /Verbosity:$verbosity
		}

	}
}

function Build-Project {
	<#
	.SYNOPSIS
	Builds project by using MSBuild
	#>
	[CmdletBinding()]
	param(
		[Parameter(Mandatory=$True, ValueFromPipeline=$True)]
		[System.IO.FileInfo]$project,
		[Parameter()]
		[string]$config,
		[Parameter()]
		[string]$platform,
		[Parameter()]
		[string]$outputPath,
		[Parameter()]
		[string]$verbosity
	)
	PROCESS {

		$projectFile = $project.fullname;
		$projectOutputPath = "$outputPath\" + $project.basename;
		$documentationFile = $project.basename + ".xml";

		exec {
			msbuild $projectFile /t:Build /p:Confifuration=$config /p:Platform=$platform /p:OutputPath=$projectOutputPath /p:DocumentationFile=$documentationFile /Verbosity:$verbosity
		}

	}
}
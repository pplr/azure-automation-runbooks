<# 
.SYNOPSIS  
    Indexes tables in a database if they have a high fragmentation 
 
.DESCRIPTION 
    This runbook indexes all of the tables in a given database if the fragmentation is 
    above a certain percentage.  
    It highlights how to break up calls into smaller chunks,  
    in this case each table in a database, and use checkpoints.  
    This allows the runbook job to resume for the next chunk of work even if the  
    fairshare feature of Azure Automation puts the job back into the queue every 30 minutes 
 
.PARAMETER SqlServer 
    Name of the SqlServer 
 
.PARAMETER Database 
    Name of the database 
     
.PARAMETER SQLCredentialName 
    Name of the Automation PowerShell credential setting from the Automation asset store.  
    This setting stores the username and password for the SQL Azure server 
 
.PARAMETER FragPercentage 
    Optional parameter for specifying over what percentage fragmentation to index database 
    Default is 20 percent 
  
 .PARAMETER RebuildOffline 
    Optional parameter to rebuild indexes offline if online fails  
    Default is false 
     
 .PARAMETER Table 
    Optional parameter for specifying a specific table to index 
    Default is all tables 
     
.PARAMETER SqlServerPort 
    Optional parameter for specifying the SQL port  
    Default is 1433 
     
.EXAMPLE 
    Update-SQLIndexRunbook -SqlServer "server.database.windows.net" -Database "Finance" -SQLCredentialName "FinanceCredentials" 
 
.EXAMPLE 
    Update-SQLIndexRunbook -SqlServer "server.database.windows.net" -Database "Finance" -SQLCredentialName "FinanceCredentials" -FragPercentage 30 
 
.EXAMPLE 
    Update-SQLIndexRunbook -SqlServer "server.database.windows.net" -Database "Finance" -SQLCredentialName "FinanceCredentials" -Table "Customers" -RebuildOffline $True 
 
.NOTES 
    AUTHORS: System Center Automation Team, Pierre Paysant-Le Roux
    LASTEDIT: Sep 25th, 2015
#> 
workflow Update-SQLIndexRunbook 
{ 
    param( 
        [parameter(Mandatory=$True)] 
        [string] $SqlServer, 
     
        [parameter(Mandatory=$True)] 
        [string] $Database, 
     
        [parameter(Mandatory=$True)] 
        [string] $SQLCredentialName, 
             
        [parameter(Mandatory=$False)] 
        [int] $FragPercentage = 20, 
 
        [parameter(Mandatory=$False)] 
        [int] $SqlServerPort = 1433, 
         
        [parameter(Mandatory=$False)] 
        [boolean] $RebuildOffline = $False, 
 
        [parameter(Mandatory=$False)] 
        [string] $Table 
                   
    ) 
 
    # Get the stored username and password from the Automation credential 
    $SqlCredential = Get-AutomationPSCredential -Name $SQLCredentialName 
    if ($SqlCredential -eq $null) 
    { 
        throw "Could not retrieve '$SQLCredentialName' credential asset. Check that you created this first in the Automation service." 
    } 
     
    $SqlUsername = $SqlCredential.UserName  
    $SqlPass = $SqlCredential.GetNetworkCredential().Password 
     
    $TableAndIndexes = Inlinescript { 
       
        # Define the connection to the SQL Database 
        $Conn = New-Object System.Data.SqlClient.SqlConnection("Server=tcp:$using:SqlServer,$using:SqlServerPort;Database=$using:Database;User ID=$using:SqlUsername;Password=$using:SqlPass;Trusted_Connection=False;Encrypt=True;Connection Timeout=30;") 
          
        # Open the SQL connection 
        $Conn.Open() 
         
        # SQL command to find indexes and their average fragmentation
	$SQLCommandString = @"
        SELECT t.name AS TableName, i.name AS IndexName, s.avg_fragmentation_in_percent
        FROM sys.dm_db_index_physical_stats (
               DB_ID(N'$Database')
             , OBJECT_ID(0)
             , NULL
             , NULL
             , NULL) AS s
        JOIN sys.indexes AS i 
        ON s.object_id = i.object_id AND s.index_id = i.index_id
		JOIN sys.tables AS t
		ON t.object_id = s.object_id;
"@

        # Return the indexes with their corresponding average fragmentation 
        $Cmd=new-object system.Data.SqlClient.SqlCommand($SQLCommandString, $Conn) 
        $Cmd.CommandTimeout=120 
         
        # Execute the SQL command 
        $FragmentedTable=New-Object system.Data.DataSet 
        $Da=New-Object system.Data.SqlClient.SqlDataAdapter($Cmd) 
        [void]$Da.fill($FragmentedTable) 
 
  
        # Return the table names that have high fragmentation 
        ForEach ($FragTable in $FragmentedTable.Tables[0]) 
        { 
            Write-Verbose ("Table name:" + $FragTable.TableName)
            Write-Verbose ("Index name:" + $FragTable.IndexName)
            Write-Verbose ("Fragmentation:" + $FragTable.Item("avg_fragmentation_in_percent"))
             
            If ($FragTable.avg_fragmentation_in_percent -ge $Using:FragPercentage) 
            { 
                # Index is fragmented. 
                # If a specific table was specified, then return this index only if it one of this table
                If ($Table -eq $null -or $Table -eq $FragTable.TableName)
		{
                    Write-Verbose ("This index has to be rebuilt")
                    $info = @{}
                    $info.TableName = $FragTable.TableName
                    $info.IndexName = $FragTable.IndexName
                    $info
                }
            } 
        } 
 
        $Conn.Close() 
    } 
 
    # Interate through indexes with high fragmentation and rebuild it 
    ForEach ($TableAndIndex in $TableAndIndexes) 
    { 
      Write-Verbose "Creating checkpoint" 
      Checkpoint-Workflow 

      $TableName = $TableAndIndex.TableName
      $IndexName = $TableAndIndex.IndexName
      Write-Verbose "Indexing $IndexName in table $TableName..."
      
      InlineScript { 
           
        $SQLCommandString = @"
        EXEC('ALTER INDEX [$Using:IndexName] ON [$Using:TableName] REBUILD with (ONLINE=ON)')
"@
 
        # Define the connection to the SQL Database 
        $Conn = New-Object System.Data.SqlClient.SqlConnection("Server=tcp:$using:SqlServer,$using:SqlServerPort;Database=$using:Database;User ID=$using:SqlUsername;Password=$using:SqlPass;Trusted_Connection=False;Encrypt=True;Connection Timeout=30;") 
         
        # Open the SQL connection 
        $Conn.Open() 
 
        # Define the SQL command to run. 
        $Cmd=new-object system.Data.SqlClient.SqlCommand($SQLCommandString, $Conn) 
        # Set the Timeout to be less than 30 minutes since the job will get queued if > 30 
        # Setting to 25 minutes to be safe. 
        $Cmd.CommandTimeout=1500 
 
        # Execute the SQL command 
        Try  
        { 
            $Ds=New-Object system.Data.DataSet 
            $Da=New-Object system.Data.SqlClient.SqlDataAdapter($Cmd) 
            [void]$Da.fill($Ds) 
        } 
        Catch 
        { 
            if (($_.Exception -match "offline") -and ($Using:RebuildOffline) ) 
            { 
                Write-Verbose ("Building table $Using:TableName offline") 
                $SQLCommandString = @" 
                EXEC('ALTER INDEX [$Using:IndexName] ON $Using:TableName REBUILD') 
"@               
 
                # Define the SQL command to run.  
                $Cmd=new-object system.Data.SqlClient.SqlCommand($SQLCommandString, $Conn) 
                # Set the Timeout to be less than 30 minutes since the job will get queued if > 30 
                # Setting to 25 minutes to be safe. 
                $Cmd.CommandTimeout=1500 
 
                # Execute the SQL command 
                $Ds=New-Object system.Data.DataSet 
                $Da=New-Object system.Data.SqlClient.SqlDataAdapter($Cmd) 
                [void]$Da.fill($Ds) 
            } 
            Else 
            { 
                # Will catch the exception here so other tables can be processed. 
                Write-Error "Index $Using:IndexName on table $Using:TableName could not be rebuilt $_" 
             } 
        } 
        # Close the SQL connection 
        $Conn.Close() 
      }   
    } 
 
    Write-Verbose "Finished Indexing" 
}
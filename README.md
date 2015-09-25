# Azure automation scripts

## Update-SQLIndexRunbook.ps1

This script is an enhancement/correction of the script available in
[Microsoft Technet
Library](https://gallery.technet.microsoft.com/scriptcenter/Indexes-tables-in-an-Azure-73a2a8ea). It
rebuilds indexes one by one instead of rebuilding all table
indexes. On large tables this reduces the risk of exeeding the
command timeout of 25 minutes.

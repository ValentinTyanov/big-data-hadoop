# big-data-hadoop
Hadoop application that goes over a sales.csv file and outputs results like sales revenue per product, number of transactions for each payment (card) type and the amount of transactions done on each day of the year (in m/d/yy format).
They can be seen in the root folder. 

To get the application running, you first need to setup a Single Node Hadoop Cluster.
The prerequisites is JDK & Maven. You need to set them as environment & path variables as well as Hadoop.
More info about this can be googled.

Then, you need to setup the actual hadoop cluster. A full guide should cover the settings for the following files:
core-site.xml, hdfs-site.xml, mapred-site.xml, yarn-site.xml

Once the hadoop cluster is configured, you need to do the following:
Go to the hadoop home folder, then under the bin folder run the following command from an elevated command propmt (as admin).
hdfs namenode -format
Then, switch to the \sbin directory and again from elevated command prompt run start-dfs.cmd -> this will open 2 more command propmts on its own
Make sure there are no errors in them
Then from the same elevated prompt run start-yarn -> this will open 2 more command propmts on its own, again make sure everything is fine.

Hadoop cluster should be configured and nodes should be started.
You can now open the project in an IDE of your choice. In Eclipse, from the Project Explorer you need to have (if not, create)
DFS Locations. Under it, you should have hadoop location (again, create one if you don't have it).
Under it, you need to create 2 folders - input & output.
The input path in the source code points to localhost:{portnumber}\input. The output path is localhost:{portnumber}\output\{jobname}. 
Jobname refers to the name of the output files after the jobs have ran. You can see in the root folder.
Thus, if using my setup, you need to add the CSV file to the input location.

You can then run the Application as Java App.
If you run into NativeAccess error or something similar when starting the app, try copying the hadoop.dll file (resides under the hadoophome/bin folder)
to Windows/System32

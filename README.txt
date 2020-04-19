There are two ways to run the project:

************ The first way is to run the shell scripts:
There are 5 server bash, and 1 client bash.
Run the 5 server bash in 5 separate terminals and then run the clients.
To test how the program works for different scenarios below:
Only 4 servers alive
only 3 servers alive.
only 2 servers alive.
The server that you killed in  the previous steps will be revived
You can simply close the terminal as related to each server.
The default server for client to request is the server running on port 30001 (server 1).
There is no mechanism to route the client to other servers if server 1 is down.
So do not bring down server 1.



 ************ The second way is to type in command lines:

The current project runs on 6 JAR files
All are in the ./out/artifacts/ folder.
For Server jars, go to each of the Server jar folder.
For client jar, go to Client_jar folder.


To start a server, open up a terminal window.
For each server, in your CLI, type in the following:
java -jar [path directory of the server JAR] [path of the server configuration file]
The configuration file is under the serverConfiguration folder.
For server 1, you should type in the path for server1conf.json
For server 2, you should type in the path for server2conf.json, and so forth.

When you run all 5 jars
The following should appear at each of the five windows:
Server 1 connection starts.
Server 2 connection starts.
Server 3 connection starts.
Server 4 connection starts.
Server 5 connection starts.


****** The five servers are running on ports 3001, 3002, 3003, 3004, 3005 by default. ******

Then open up the 6th terminal window, and type in the following:

java -jar [path directory of the client JAR] [server port you want to connect with] [sampLe run txt file path]

For example, if the client wants to connect with server on port 3001,
and the sample run txt file is ClientRequest.txt,
then specify it in your arguments, such as:

Java -jar 3001 ./src/main/java/com/github/jiali/paxos/clientRequests/ClientRequest.txt

To switch to other sample runs (e.g., ClientRequestGetOnly.txt, ClientRequestPutOnly, ClientRequestDeleteOnly), specify as instructed.
These existing sample runs are in the clientRequests folder.

The ClientRequest.txt sample run consists of a mix of at least five of each operation: 5 PUTs, 5 GETs, 5 DELETEs.

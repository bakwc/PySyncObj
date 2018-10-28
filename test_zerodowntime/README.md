test.py is a script to test zero-downtime upgrades between two versions of the code. Use `python3 test.py -h` to see its options.

The basic operation is that test.py spawns a cluster on the local machine. This cluster is simply a distributed counter. test.py then sends increment commands to the cluster processes in a random (but controllable) way. One after the other, it also takes a process down, upgrades its code, and restarts it again a bit later. The other processes continue working, i.e. the cluster should still be functional ("zero downtime"). At the end, the Raft logs and counter values from all processes are compared to check that everything was working correctly.

proc.py is the script executed by the individual processes spawned by test.py. It takes commands via stdin and replies via stdout.

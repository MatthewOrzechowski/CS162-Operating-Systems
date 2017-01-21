# CS162-Operating-Systems
Repository for final versions of work for CS162 Operating Systems

This repository contains much of the work completed for the CS162 Operating Systems class.
It contains 3 projects, worked on in a group with 2 other students.
Projects 1 and 2 were done directly in an educational operating system written by Stanford called "PintOS", 
and project 3 was unrelated to that system itself, but was still ran and tested on our system that was worked on
throughout projects 1 and 2. 

The first project, mostly in pintos/src/threads/thread.c, among other files, required the implementation of a 
timer interrupter, a priority scheduler with priority donation to resolve any blocked threads waiting on resources
from threads with lower priority, as well as an advanced scheduler that helped distribute parity among threads by 
decreasing priority the longer the thread was in existence, allowing other threads to execute.

The second project, contained in pintos/src/userprog, required the implementation of argument loading and process 
executing. This required the loading of arguments into memory, loading the program into memory, executing the 
programs, as well as ensuring that the parent calling process would resume execution upon completion of the child
process.

The third project, contained in kvstore, required the implementation of two Key-Value storage systems, mostly in kvstore/src/server. The first was a more straightforward distributed system, involving hashing and load balancing to ensure that each server in the cluster would receive proportially equal put and get requests, as well as ensuring that any communcation between those servers through socket servers was operational, as well as concurrent. The second system was built upon the first, maintaining all synchroinzation and concurrency, as well as the dirstubuted load hashing, but also introduced Two Phase Committing and the ability to rebuild a crashed server from its log. Two Phase Committing ensures
that all data is consistent across a distributed system by having a master server, in this case TPCMASTER, issue
a request to vote to the other slave servers whenever a put request was issued. Then the slave servers would 
issue their response to the request, and the TPCMASTER would only issue the final commit order to the slaves if
all the slaves agreed to commit. If any slave at any time were to crash, the TPCMASTER would not issue the final
commit order until the slave was rebuilt from its log and then issued its commit vote. In this way, data is always
ensured to be consistent across all slave servers, and each server is able to begin from the exact point at which
it crashed in the process.

If you have any questions on any files or ideas, please feel free to email me at morzechowski (at) berkeley (dot) (edu)

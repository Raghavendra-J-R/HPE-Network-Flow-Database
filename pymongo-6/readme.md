In this week I have corrected the errors in the previous folders - pymongo-3, pymongo-4, pymongo-5 

Previously I used to include start time during the file read itself which used to get added to the CRUD latency and had kept the end time outside the for loop as a result of which I used to get erroneous 
values out of it. 
Now I have updated the code so as to rectify the mistake and I have been getting proper readings


Current week's data 


![image](https://github.com/Raghavendra-J-R/Pymongo/assets/121816454/1331971e-f2d3-44be-a24e-c12afe7046de)


Comparison needs to be done with YCSB custom workload with *readmodifywriteproportion = 1*

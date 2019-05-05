# context aware recommendation for resource allocation

The code base has the following 
1. Parses the MXML format to create a database in mysql
2. Preprocesses the event log to create a start and end time per task
3. Infers contextual data from the event logs such as experience, preference, workload
4. Generates a rating per task completion based on completion time
5. Generates the output required for context aware recommendation - tasks, resources, contextual attributes, rating
6. Runs CARSKit to predict the rating of resources on tasks for a context using k-fold cross validation


The approach has been used in:
Renuka Sindhgatta, Aditya K. Ghose, Hoa Khanh Dam:
Context-Aware Recommendation of Task Allocations in Service Systems. ICSOC 2016: 402-416

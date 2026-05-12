I want to create a worflow orchestrator tool where it will have these tools:
action nodes

1. write file
2. read file
3. move file to dir
4. list files in dir

I have condition nodes :

Bases on conditions of the file what action is to be performed for example if the characters in the file or words in the file or certains words in some file .

The Engine:

the engine should be able to execute multiple instances workflows in parallel or a job with a workflow can trigger multiple jobs . for example with a file condition i have to read files in dir and write something during that time i should run multiple instances . If the instances is greater than the max instances that we defined in the start it should be queued  . 

Right now no need of any db store then in Db dir in form of jsons :


1. jobs
2. nodes
3. workflows


I want a manual trigger using cli keep it in the root . 

even i should be able to get status of workflow 








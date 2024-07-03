# Please add your team members' names here. 

## Team members' names 

1. Student Name: Hannah Kim

   Student UT EID: hk25684

2. Student Name: Noah Hodge

   Student UT EID: nah2664

3. Student Name: Janet Kim

   Student UT EID: jk48674

4. Student Name: Swetha Tandri

   Student UT EID: sgt635

 ...

##  Course Name: CS378 - Cloud Computing 

##  Unique Number: 84744
    


# Add your Project REPORT HERE

# Task 1: simple linear regression
```bash
Small data:
variable m	2.3693504
variable b	8.64382

Large data:
Completion time : 27 min 27 sec
variable m	2.2364414
variable b	10.990246

```

# Task 2: parameters using gradient descent
```bash
Small data:

Large data:

```

# Task 3: fit multiple linear regression using gradient descent
```bash
Small data:

Large data:

```


# Project Template

# Running on Laptop     ####

Prerequisite:

- Maven 3

- JDK 1.6 or higher

- (If working with eclipse) Eclipse with m2eclipse plugin installed


The java main class is:

edu.cs.utexas.HadoopEx.WordCount 

Input file:  Book-Tiny.txt  

Specify your own Output directory like 

# Running:




## Create a JAR Using Maven 

To compile the project and create a single jar file with all dependencies: 
	
```	mvn clean package ```



## Run your application
Inside your shell with Hadoop

Running as Java Application:

```java -jar target/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar SOME-Text-Fiel.txt  output``` 

Or has hadoop application

```hadoop jar your-hadoop-application.jar edu.cs.utexas.HadoopEx.WordCount arg0 arg1 ... ```



## Create a single JAR File from eclipse



Create a single gar file with eclipse 

*  File export -> export  -> export as binary ->  "Extract generated libraries into generated JAR"

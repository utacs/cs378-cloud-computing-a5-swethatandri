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
```

```bash
Large data:
Completion time : 27 min 27 sec
variable m	2.2364414
variable b	10.990246

```

# Task 2: parameters using gradient descent
```bash
Small data:
Cost : 54.87096141091008
Convergence
Final m : 2.3689076533559996
Final b : 8.649912112565776
```

```bash
Large data:
m	2.3739809429446983
b	8.995136677271985
Cost	52.84244782323212
```

# Task 3: fit multiple linear regression using gradient descent
```bash
Small data:
Cost : 23.465757322607804
Final Values: w0: 10.809542789703205, w1: 5.031754250565101E-4, w2: 1.477341081780779, w3: 0.4476357857514438, w4: 0.3239640191951284
```
```bash
Large data:
w0	2.288966097157886E13
w1	4.6388377958070306E14
w2	3.0635982493296235E12
w3	9.47205648362914E12
w4	1.278892732643992E12
Cost	5.627965685601388E35
```
# Google Cloud - Dataproc/Yarn History
https://docs.google.com/document/d/1NhJ80xJW_28ZKf_R2UNSN9CyK_vB9tOmrI0ZW983dxc/edit?usp=sharing

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

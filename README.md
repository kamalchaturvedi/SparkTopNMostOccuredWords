**SparkTopNMostOccuredWords**

This the implementation of Apache Spark in Java, in order to find the top N most occured words in a bigData file, by 
exercising the amazing spark APIs. There is detailed explaination of what task each line in code performs, in the Java file.  

**Build Jar** : You can build the jar with the below mentioned command
    maven clean install
**Input** : You can run the spark job in a stand-alone node by the command 

    spark-submit --class "com.kamal.SparkWordCount.SparkWordCountApplication" --master local[2] "./SparkWordCount.jar" ./big.txt ./output 10

Where you can replace ./big.txt with the input file path
                      ./output with the output file path &
                      10 with the value of N (For the usecase : Top N most occured words)

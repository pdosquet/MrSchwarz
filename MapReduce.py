from operator import add
import time
from mrjob.job import MRJob

class MRSparkWordcount(MRJob):
    def __init__(self):
        self.spark()
    
    def processText(self,line):
        start = 0
        while start != -1:
            
            start = line.find("Twitter for", start)

            if start != -1:
                end = line.find("\\u003c\\", start)
                os = line[(start+len("Twitter for")+1):end]
                if len(os) > 0:
                    yield os
                start = end

    def spark(self):
        # Spark may not be available where script is launched
        from pyspark import SparkContext


        time_start_wall = time.time()
        time_start_cpu = time.process_time()
        with open('test/collectedtweets.txt','r') as f:
            lines = f.readlines()

        chunk_size = 100

        sc = SparkContext(appName='mrjob Spark wordcount script')

        final_counts=[]

        for i in range(0,len(lines)-chunk_size,chunk_size):
            lines_rdd = sc.parallelize(lines[i:i+chunk_size])
            lines_rdd.cache()
            os_counts = (lines_rdd.flatMap(self.processText).map(lambda x: (x,1))
                .reduceByKey(add))
            final_counts.extend(os_counts.collect())
            lines_rdd.unpersist()

        final_counts_rdd = sc.parallelize(final_counts)
        final_counts_ordered = final_counts_rdd.reduceByKey(add).sortBy(lambda x: -x[1]).collect()

        sc.stop()

        time_end_wall = time.time()
        time_end_cpu = time.process_time()

        for os, count in final_counts_ordered:
            print(f"{os}: {count}")
        
        print("Wall Time: "+ str(time_end_wall - time_start_wall))
        print("CPU Time: " + str(time_end_cpu - time_start_cpu) )


if __name__ == '__main__':
    inst = MRSparkWordcount()



#docker run --rm -v '/c/Users/pierr/OneDrive - Universite de Liege/M1/Advanced Topics In Digital Business/Projet/MapReduce/Docker/pyspark-Docker-master':'/test' mapreduce
#docker run -it --rm -v '/c/Users/pierr/OneDrive - Universite de Liege/M1/Advanced Topics In Digital Business/Projet/MapReduce/Docker/pyspark-Docker-master':'/test' mapreduce
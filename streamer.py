from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def aggregate_tags_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def main():

    # initilize SparkContext
    sc = SparkContext("local[2]", "TwitterStreaming")
    sc.setLogLevel("ERROR")

    # initialize StreamingContext
    ssc = StreamingContext(sc, 1)

    # setting a checkpoint to allow RDD recovery
    ssc.checkpoint("checkpoint_TwitterApp")
    # read data from port 9009
    dataStream = ssc.socketTextStream("localhost",9009)



    # split each tweet into words
    words = dataStream.flatMap(lambda line: line.split(" "))
    # filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
    hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
    # adding the count of each hashtag to its last count
    tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
    # do processing for each RDD generated in each interval
    tags_totals.foreachRDD(process_rdd)
    # start the streaming computation
    ssc.start()
    # wait for the streaming to finish
    ssc.awaitTermination()



if __name__ == '__main__':
    main()    
import sys
from colour_tools import colour_schema, rgb2lab_query, plot_predictions
from pyspark.sql import SparkSession, functions, types
from pyspark.ml import Pipeline


spark = SparkSession.builder.appName('colour predicter').getOrCreate()
# sc = spark.sparkContext
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert spark.version >= '2.2'  # make sure we have Spark 2.2+


def main(inputs):
    data = spark.read.csv(inputs, header=True, schema=colour_schema)
    lab_query = rgb2lab_query(passthrough_columns=['labelword'])

    # TODO: actually build the components for the pipelines, and the pipelines.
    #indexer = 
    #rgb_assembler = 
    
    models = [
        #('RGB-forest', Pipeline(stages=[])),
        #('LAB-forest', Pipeline(stages=[])),
        #('RGB-MLP', Pipeline(stages=[])),
        #('LAB-MLP', Pipeline(stages=[])),
    ]

    # TODO: need an evaluator
    #evaluator = 

    # TODO: split data into training and testing
    #train, test = 
    train = train.cache()
    test = test.cache()
    for label, pipeline in models:
        # TODO: fit the pipeline to create a model
        #model =
        
        # Output a visual representation of the predictions we're
        # making: uncomment when you have a model working
        #plot_predictions(model, label)

        # TODO: predict on the test data
        #predictions = 
        
        # calculate a score
        #score = evaluator.evaluate(predictions)
        #print(label, score)


if __name__ == "__main__":
    inputs = sys.argv[1]
    #output = sys.argv[2]
    main(inputs)

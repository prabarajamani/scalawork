from pyspark.ml.util import keyword_only
from pyspark.ml.pipeline import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
# Create a custom word count transformer class

from pyspark.ml import Pipeline
from pyspark.ml.feature import *
from pyspark.ml.classification import LogisticRegression
# Configure pipeline stages
tok = Tokenizer(inputCol="review", outputCol="words")
htf = HashingTF(inputCol="words", outputCol="tf", numFeatures=200)
w2v = Word2Vec(inputCol="review", outputCol="w2v")
ohe = OneHotEncoder(inputCol="rating", outputCol="rc")
va = VectorAssembler(inputCols=["tf", "w2v", "rc"], outputCol="features")
lr = LogisticRegression(maxIter=10, regParam=0.01)
# Build the pipeline
pipeline = Pipeline(stages=[tok, htf, w2v, ohe, va, lr])
# Fit the pipeline
model = pipeline.fit(train_df)
# Make a prediction
prediction = model.transform(test_df)

class MyWordCounter(Transformer, HasInputCol, HasOutputCol):
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(WordCounter, self).__init__()
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)
    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)
    def _transform(self, dataset):
        out_col = self.getOutputCol()
        in_col = dataset[self.getInputCol()]
    # Define transformer
    logic def f(s):
        return len(s.split(' '))
    t = LongType()
    return dataset.withColumn(out_col, udf(f, t)(in_col))
# Instantiate the new word count transformer
wc = MyWordCounter(inputCol="review", outputCol="wc")
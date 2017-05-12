from kesaiRecommend.srcs.sc import SparkContexts
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.recommendation import Rating
import numpy as np

class ItemALS:

    def item_ALS(self):
        sc = SparkContexts.SparkContexts().get_sc()
        print 'test1 '
        rating_data = sc.textFile("../../../data/ml-100k/u.data")
        rating_fields = rating_data.map(lambda x: x.split("\t"))
        ratings = rating_fields.map(lambda x: x[:3])
        Ratings = ratings.map(lambda x: Rating(int(x[0]), int(x[1]), float(x[2])))
        model = ALS.train(Ratings, 50, 10, 0.01)
        itemId = 567

        itemFactor = model.productFeatures().lookup(itemId)[0]
        item_array = np.array(itemFactor)
        sim = model.productFeatures().map(lambda (id, Factor): (id, cosineSimilarity(np.array(Factor), item_array)))
        result = sim.sortBy(lambda (x, y): y, ascending=False).take(10)

        def cosineSimilarity(v1, v2):
            return np.dot(v1, v2) / np.linalg.norm(v1) * np.linalg.norm(v2)

        # item_name
        item_names = self.item_name(sc)
        item_result = []
        print 'item_Factor ', item_names[str(itemId)]
        for item in result:
            name = item_names[str(item[0])]
            tmp = (name, item[1])
            item_result.append(tmp)

        print item_result

    def item_name(sc,self):
        movies_data = sc.textFile("../../../data/ml-100k/u.item")
        movies_tmp = movies_data.map(lambda x: x.split("|"))
        item_names = movies_tmp.map(lambda x: (x[0], x[1])).collectAsMap()
        print "item_names ", item_names
        return item_names

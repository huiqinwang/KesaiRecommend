# -*- coding:UTF-8 -*-
from kesaiRecommend.srcs.sc import SparkContexts
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.recommendation import Rating
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.mllib.evaluation import RankingMetrics

import numpy as np

class UserALS:

    def als(self):
        sc = SparkContexts.SparkContexts().get_sc()
        print 'test1 '
        rating_data = sc.textFile("../../../data/ml-100k/u.data")
        rating_fields = rating_data.map(lambda x: x.split("\t"))
        ratings = rating_fields.map(lambda x: x[:3])
        Ratings = ratings.map(lambda x: Rating(int(x[0]), int(x[1]), float(x[2])))
        model = ALS.train(Ratings, 50, 10, 0.01)

        print model.userFeatures().count()
        print model.productFeatures().count()
        print model.userFeatures().take(2)
        print model.productFeatures().take(2)
        print model.predict(789, 123)
        # 推荐
        top_res = model.recommendProducts(789, 10)
        print top_res
        # model_IM = ALS.trainImplicit(Ratings)
        movies_data = sc.textFile("../../../data/ml-100k/u.item")
        movies_tmp = movies_data.map(lambda x: x.split("|"))
        movies_item = movies_tmp.map(lambda x: (int(x[0]), x[1])).collectAsMap()

        print movies_item
        movie_user_item = Ratings.keyBy(lambda x: x[0]).lookup(789)
        print len(movie_user_item)
        movie_user_k = Ratings.sortBy(lambda x: x[2], ascending=False).take(10)
        movie_product = []
        # 实际
        for item in movie_user_k:
            tmp = (movies_item[item[1]], item[2])
            movie_product.append(tmp)

        print movie_product

        # MSE
        user_product = Ratings.map(lambda x: (x.user, x.product))
        predictions = model.predictAll(user_product).map(lambda x: ((x.user, x.product), x.rating))
        ratings_act_prediction = Ratings.map(lambda x: ((x.user, x.product), x.rating)).join(predictions)
        MSE = ratings_act_prediction.map(lambda ((x, y), (m, n)): np.power(m - n, 2)).reduce(
            lambda x, y: x + y) / ratings_act_prediction.count()
        print "MSE", MSE
        print 'sqrt', np.sqrt(MSE)

        # MAP
        actualMovies = [x.product for x in movie_user_item]
        predictMovies = [x.product for x in top_res]
        MAP10 = self.MAP(actualMovies, predictMovies, 10)
        print 'MAPK', MAP10

        # mlMSE
        predictAndTrue = ratings_act_prediction.map(lambda ((x, y), (m, n)): (m, n))
        regressMatrix = RegressionMetrics(predictAndTrue)
        print 'ml MSE', regressMatrix.meanSquaredError
        print 'ml MSEs', regressMatrix.rootMeanSquaredError

        # rank
        itemFactor = model.productFeatures().map(lambda (id, factor): factor).collect()
        item_matrixs = np.array(itemFactor)

        imBroadcast = sc.broadcast(item_matrixs)
        user_vector = model.userFeatures().map(lambda (id, factor): (id, np.array(factor)))
        user_vector = user_vector.map(lambda (id, x): (id, imBroadcast.value.dot(np.array(x).transpose())))
        user_vector_id = user_vector.map(lambda (id, x): (id, [(xx, i) for i, xx in enumerate(list(x))]))
        sort_user_recid = user_vector_id.map(lambda (id, x): (id, sorted(x, key=lambda x: x[0], reverse=True)))
        user_movie = Ratings.map(lambda x: (x.user, x.product)).groupBy(lambda (x, y): x)
        sort_lables = sort_user_recid.join(user_movie).map(lambda (id, (pre, acu)): (pre, acu))
        rank_metrix = RankingMetrics(sort_lables)
        print 'm a p', rank_metrix.meanAveragePrecision
        print 'map k = 10', rank_metrix.precisionAt(10)

    def MAP(self,acu, pre, k):
        num = 0
        score = 0
        if len(acu) > k:
            acuk = acu[:k + 1]
        else:
            acuk = acu
        for i, p in enumerate(acuk):
            if p in acuk and p not in pre:
                num += 1
                score += num / (float(i) + 1.0)
        if not acuk:
            return 1.0
        else:
            return score / min(len(acuk), k)

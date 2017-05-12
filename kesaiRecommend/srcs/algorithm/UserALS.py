# -*- coding:UTF-8 -*-
from kesaiRecommend.srcs.sc import SparkContexts
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.recommendation import Rating
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.mllib.evaluation import RankingMetrics
import numpy as np
import time

class UserALS:
    def __init__(self):
        self.sc = SparkContexts.SparkContexts().get_sc()
        self.rating_file = self.sc.textFile("../../../data/preprocessor/user_item_score.txt")
        # self.rating_file = self.sc.textFile("../../../data/preprocessor/test_score.txt")

    # 显shi ALS模型
    def get_model(self):
        print 'test1 '
        # rating_data = self.sc.textFile("../../../data/preprocessor/user_item_count.txt")
        rating_fields = self.rating_file.map(lambda x: x.split(","))
        ratings = rating_fields.map(lambda x: x[:3])
        Ratings = ratings.map(lambda x: Rating(int(x[0]), int(x[1]), float(x[2])))
        model = ALS.train(Ratings, 50, 10, 0.01)

        print model.userFeatures().count()
        print model.productFeatures().count()
        print model.userFeatures().take(2)
        print model.productFeatures().take(2)
        # print model.predict(789, 123)

        return model,Ratings

    # 进行推荐
    def recommend_user(self,model,Ratings,user_dict,item_dict):
        # {user_name|item_name:probability}
        recommend_file = "../../../data/result/total_recommend.txt"
        recommend_write = open(recommend_file,"wb")

        # 字典翻转{user_id:user_name}
        user_turn_dict = {v:k for k,v in user_dict.items()}
        item_turn_dict = {v:k for k,v in item_dict.items()}

        # 用户已经看过的item，删除:猜想：一个人看过了第二天再看的概率很小
        rating_map = Ratings.map(lambda x:(x.user,x.product)).reduceByKey(lambda x,y:list(x).append(y))
        print "rating_map ",rating_map.take(10)




        # for item in user_turn_dict.keys():
        #     movie_user_item = Ratings.keyBy(lambda x: x[0]).lookup(int(item))
        #     # 确保一定有5个
        #     k = 5 + len(movie_user_item)
        #     top_res = model.recommendProducts(int(item),k) # Rating(789,715,5.931851273771102)
        #     # print "top_res_sample",top_res
        #     # 已评价过的item
        #     # print "user_item",movie_user_item
        #
        #     # 删除已评价过测item
        #     top_count = []
        #     user_item = []
        #     count = 0
        #     for use in user_item:
        #         user_item.append(use)
        #     for rat in top_res:
        #         if rat.product not in user_item and count < 5:
        #             top_count.append(rat)
        #             count += 1
        #         elif count > 5:
        #             break
        #
        #     strs = str(top_count[0].user)
        #     for item in top_count:
        #         strs += "|" + str(item.product)+":"+str(item.rating)
        #     print "strs ",strs
        #     strs += "\r\n"
        #     recommend_write.write(strs)

        recommend_write.close()

    # 进行测试推荐
    def recommend_user_test(self,model,Ratings,user_dict,item_dict,test_user):
        # {user_name|item_name:probability}
        recommend_file = "../../../data/result/total_recommend.txt"
        recommend_write = open(recommend_file,"wb")

        # 字典翻转{user_id:user_name}
        user_turn_dict = {v:k for k,v in user_dict.items()}
        item_turn_dict = {v:k for k,v in item_dict.items()}

        # 提前将用户评价过的物品统计
        rating_fields = self.rating_file.map(lambda x: x.split("\t"))
        ratings = rating_fields.map(lambda x: x[:2])

        for item in user_turn_dict.keys():
            movie_user_item = Ratings.keyBy(lambda x: x[0]).lookup(int(item))
            # 确保一定有5个
            k = 5 + len(movie_user_item)
            top_res = model.recommendProducts(int(item),k) # Rating(789,715,5.931851273771102)
            # print "top_res_sample",top_res
            # 已评价过的item
            # print "user_item",movie_user_item

            # 删除已评价过测item
            top_count = []
            user_item = []
            count = 0
            for use in user_item:
                user_item.append(use)
            for rat in top_res:
                if rat.product not in user_item and count < 5:
                    top_count.append(rat)
                    count += 1
                elif count > 5:
                    break

            strs = str(top_count[0].user)
            for item in top_count:
                strs += "|" + str(item.product)+":"+str(item.rating)
            # print "strs ",strs
            strs += "\r\n"
            recommend_write.write(strs)

        recommend_write.close()

    def MAP_k_lib(self):
        pass

    # def MAP_K_self(self,Ratings,model,user_list):
    #     # MSE
    #     user_product = Ratings.map(lambda x: (x.user, x.product))
    #     predictions = model.predictAll(user_product).map(lambda x: ((x.user, x.product), x.rating))
    #     ratings_act_prediction = Ratings.map(lambda x: ((x.user, x.product), x.rating)).join(predictions)
    #     MSE = ratings_act_prediction.map(lambda ((x, y), (m, n)): np.power(m - n, 2)).reduce(
    #         lambda x, y: x + y) / ratings_act_prediction.count()
    #     print "MSE", MSE
    #     print 'sqrt', np.sqrt(MSE)
    #
    #     # MAP
    #     actualMovies = [x.product for x in movie_user_item]
    #     predictMovies = [x.product for x in top_res]
    #     MAP10 = self.MAP(actualMovies, predictMovies, 10)
    #     print 'MAPK', MAP10
    #
    #     # mlMSE
    #     predictAndTrue = ratings_act_prediction.map(lambda ((x, y), (m, n)): (m, n))
    #     regressMatrix = RegressionMetrics(predictAndTrue)
    #     print 'ml MSE', regressMatrix.meanSquaredError
    #     print 'ml MSEs', regressMatrix.rootMeanSquaredError
    #
    #     # rank
    #     itemFactor = model.productFeatures().map(lambda (id, factor): factor).collect()
    #     item_matrixs = np.array(itemFactor)
    #
    #     imBroadcast = sc.broadcast(item_matrixs)
    #     user_vector = model.userFeatures().map(lambda (id, factor): (id, np.array(factor)))
    #     user_vector = user_vector.map(lambda (id, x): (id, imBroadcast.value.dot(np.array(x).transpose())))
    #     user_vector_id = user_vector.map(lambda (id, x): (id, [(xx, i) for i, xx in enumerate(list(x))]))
    #     sort_user_recid = user_vector_id.map(lambda (id, x): (id, sorted(x, key=lambda x: x[0], reverse=True)))
    #     user_movie = Ratings.map(lambda x: (x.user, x.product)).groupBy(lambda (x, y): x)
    #     sort_lables = sort_user_recid.join(user_movie).map(lambda (id, (pre, acu)): (pre, acu))
    #     rank_metrix = RankingMetrics(sort_lables)
    #     print 'm a p', rank_metrix.meanAveragePrecision
    #     print 'map k = 10', rank_metrix.precisionAt(10)

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

if __name__ == "__main__":
    uals = UserALS()
    start = time.time()

    import kesaiRecommend.srcs.preprocessor.NewsDict as nd
    import  kesaiRecommend.srcs.preprocessor.UsersDict as ud
    model,Ratings = uals.get_model()
    users_dict = ud.UsersDict().users_dict()
    news_dict = nd.NewsDict().news_dict()


    uals.recommend_user(model,Ratings,users_dict,news_dict)

    end = time.time()
    print "time ",end -start
# -*- coding:UTF-8 -*-
import numpy as np
from matplotlib import pyplot as plt
from kesaiRecommend.srcs.sc import SparkContexts as scs

class RatingData:

    def rating_data(self):
        s = scs.SparkContexts()
        sc = s.get_sc()
        rating_data = sc.textFile("../../../data/preprocessor/user_item_score.txt")
        num_rating = rating_data.count()
        print 'rating_num ', num_rating
        rating_fields = rating_data.map(lambda x: x.split(","))
        return rating_fields, num_rating

    def rating_statics(self):
        rating_field, num_rating = self.rating_data()
        rating = rating_field.map(lambda x: int(x[2]))
        max_rating = rating.reduce(lambda x, y: max(x, y))
        min_rating = rating.reduce(lambda x, y: min(x, y))
        mean_rating = rating.reduce(lambda x, y: x + y) / num_rating
        median_rating = np.median(rating.collect())
        rating_per_user = num_rating / 943
        rating_per_item = num_rating / 1682
        print 'max', max_rating
        print 'min ', min_rating
        print 'mean ', mean_rating
        print 'median ', median_rating
        print 'average per user ', rating_per_user
        print 'average per item ', rating_per_item
        print 'states ', rating.stats()
        return rating

    # 对item评价分布
    def draw_rating(self):
        rating = self.rating_statics()
        rating_count = rating.countByValue()
        x_axis = np.array(rating_count.keys())
        y_axis = np.array([float(x) for x in rating_count.values()])

        y_axis = y_axis / y_axis.sum()
        pos = np.arange(len(y_axis))
        width = 1.0

        ax = plt.axes()
        ax.set_xticks(pos + (width / 2))
        ax.set_xticklabels(x_axis)

        plt.bar(pos, y_axis, width, color='lightblue')
        fig = plt.gcf()
        fig.set_size_inches(16, 10)
        plt.show()

    # 对各个item的评级次数,这里不行，数据不合适
    def user_rating(self):
        rating_fields, num_rating = self.rating_data()
        user_rating = rating_fields.map(lambda x: (int(x[0]), int(x[2]))).groupByKey()
        print "user_rating ", user_rating.take(2)
        user_rating_by = user_rating.map(lambda (k, v): (k, len(v)))
        print user_rating_by.take(5)
        user_rating_by_local = user_rating_by.map(lambda (k, v): v).collect()
        plt.hist(user_rating_by_local, bins=200, color="lightblue", normed=True)
        fig = plt.gcf()
        fig.set_size_inches(26, 20)
        plt.show()


if __name__ == "__main__":
    rating = RatingData()
    rating.draw_rating()
    '''
    rating_num  624044
    max 12
    min  1
    mean  2
    median  3.0
    average per user  661
    average per item  371
    states  (count: 624044, mean: 2.70862150746, stdev: 0.806589596287, max: 12.0, min: 1.0)
    '''

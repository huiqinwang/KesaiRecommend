# -*- coding:UTF-8 -*-
import  csv

# 创建咨询字典{item_name:item_id}
class NewsDict:

    def news_dict(self):
        news_file = '../../../data/origin/news_info.csv'

        # news_info csv文件最好用csv包处理
        news_files = file(news_file)
        news_reader = csv.reader(news_files)
        news_data = []
        for item in news_reader:
            news_data.append(item[0])
        news_data.pop(0) # title
        print 'new_data',len(news_data)
        print 'sample ',news_data[0:10]
        news_files.close()

        # 将ID做成字典
        news_dict = {}
        for i in range(1,len(news_data)+1):
            news_dict.setdefault(news_data[i-1],"")
            news_dict[news_data[i-1]]= str(i)

        return  news_dict

if __name__ == "__main__":
    news = NewsDict()
    news_dict = news.news_dict()
    print "news_dict",len(news_dict.keys())
    print "news_dict_sample",news_dict["530202"]
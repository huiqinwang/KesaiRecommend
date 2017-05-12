# -*- coding:UTF-8 -*-
import csv
import time
import NewsDict as nd
import UsersDict as ud


class ScaleData:
    # 获取评论数据
    def train_scale(self):
        # 获取newsdict users_dict
        news_dict = nd.NewsDict().news_dict()
        users_dict = ud.UsersDict().users_dict()

        #测试逻辑
        train_file = "../../../data/origin/train.csv"
        action_count_file = "../../../data/preprocessor/user_item_count.txt"
        score_file = "../../../data/preprocessor/user_item_score.txt"

        # 获得字典{user_id:{{item_id:{action_id1:counts,action_id1:0}},{item_id2:{action_id1:0,action_id1:0}}}}
        dict = {}
        train_files = file(train_file)
        train_reader = csv.reader(train_files)
        for train in train_reader:
            user_id = dict.setdefault(train[0],{})
            # print "user_id",user_id
            item_id = user_id.setdefault(train[1],{})
            action_id = item_id.setdefault(train[3],1)
            if action_id > 1:
                dict[train[0]][train[1]][train[3]] += 1
        # 5325: 3000个用户对
        print "dict_len",len(dict.keys())
        print "dict_sample",dict["0365F7AE-5048-42B3-BB2C-8E637A380A3E"]

        # 保存用户对每个item的行为count数据:user:item|view:1|collect:2|
        action_count_write = open(action_count_file,"w")
        for user_id in dict.keys():
            for item_id in dict[user_id].keys():
                action_count_line = str()
                action_count_line += users_dict[user_id]
                action_count_line += "|"+news_dict[item_id]
                for action_id in dict[user_id][item_id].keys():
                    action_count_line += "|"+action_id+":"+str(dict[user_id][item_id][action_id])
                action_count_line += "\r\n"
                # print "action_count_line",action_count_line
                action_count_write.write(action_count_line)
        action_count_write.close()


        #{user:{{item:score},{items:score2}}}
        score_dict = {}
        # view、deep_view、share、comment、collect
        weight_action = {"view":1,"deep_view":2,"comment":3,"collect":4,"share":5}
        for user_id in dict.keys():
            score_dict.setdefault(user_id,{})
            for item_id in dict[user_id].keys():
                score_dict[user_id].setdefault(item_id,0)
                action_sum = 0
                for action_id in dict[user_id][item_id].keys():
                    action_count = dict[user_id][item_id][action_id]
                    action_sum += weight_action[action_id]*int(action_count)
                score_dict[user_id][item_id] = action_sum
        print "score_len",len(score_dict.keys())
        print "score_dict_sample",score_dict["0365F7AE-5048-42B3-BB2C-8E637A380A3E"]

        # 保存评分数据： user,item,score
        score_write = open(score_file,"wb")
        for user_id in score_dict.keys():
            for item_id in score_dict[user_id].keys():
                score_line = users_dict[user_id]
                score_line += ","+news_dict[item_id]+","+str(score_dict[user_id][item_id])
                score_line += "\r\n"
                # print "score_line",score_line
                score_write.write(score_line)

        score_write.close()

        print "ok!"


        # 保存评分字典
        # file_name = ["userID"]
        # file_name.extend(train_news)
        # score_file = file( score_file,"w")
        # score_write = csv.DictWriter(score_file,file_name)
        # # 写入头文件
        # score_write.writeheader()
        #
        # print "train_news",len(train_news)
        # for user_id in score_dict.keys():
        #     news_score = {}
        #     user_news = news_score.setdefault("userID",user_id)
        #     for item_id in train_news:
        #         if item_id not in score_dict[user_id].keys():
        #             news_score.setdefault(item_id,0)
        #         else:
        #             print "exist"
        #             news_score.setdefault(item_id,score_dict[user_id][item_id])
        #     print "lens_newscore",len(news_score)
        #     score_write.writerow(news_score)
        # score_file.close()

if __name__ == "__main__":
    # 总处理时间4s
    start = time.time()

    train = ScaleData()
    train.train_scale()

    end = time.time()
    print "time ",end -start





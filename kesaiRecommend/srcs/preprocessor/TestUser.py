# -*- coding:UTF-8 -*-

# 获取测试数据的用户id
class TestUser:

    def get_userID(self):
        test_file = "../../../data/origin/test.txt"
        test_read = open(test_file)

        user_list = []
        for line in test_read.readlines():
            line_split = line.split(",")
            user_list.append(line_split[0])

        return  user_list
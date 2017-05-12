# -*- coding:UTF-8 -*-

# 创建用户字典：{user_name:user_id}
class UsersDict:

    def users_dict(self):
        users_file = '../../../data/origin/candidate.txt'

        # users_info csv文件最好用csv包处理
        users_reader = open(users_file)
        users_lines = users_reader.readlines()
        users_data = []
        for item in users_lines:
            users_data.append(item.strip())
        print 'users_data',len(users_data)
        print 'sample ',users_data[0:10]
        users_reader.close()

        # 将ID做成字典
        users_dict = {}
        for i in range(1,len(users_data)+1):
            users_dict.setdefault(users_data[i-1],"")
            users_dict[users_data[i-1]] = str(i)
        return  users_dict



if __name__ == "__main__":
    users = UsersDict()
    users_dict = users.users_dict()
    print "users_dict ",len(users_dict.keys())
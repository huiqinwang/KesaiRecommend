# -*- coding:UTF-8 -*-
class Singleton(object):
    __instance = None
    def __init__(self):
        pass
    def __new__(cls, *args, **kwargs):
        if Singleton.__instance is None:
            Singleton.__instance = object.__new__(cls,*args, **kwargs)
        return Singleton.__instance

class A(Singleton):
    __sc = None
    def __init__(self):
        self.__sc = 1

    def get_sc(self):
        return self.__sc


if __name__ == "__main__":
    a = A()
    b = A()
    print id(a),a.get_sc()
    print id(b)
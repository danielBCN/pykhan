import redis


class RedisConn(object):
    REDIS = None

    @classmethod
    def set_up(cls, host='localhost', port=6379, password=''):
        cls.REDIS = redis.ConnectionPool(
            host=host, port=port, password=password)
        return cls.get_conn()

    @classmethod
    def get_conn(cls):
        return redis.Redis(connection_pool=cls.REDIS)


class RedisBarrier(object):

    def __init__(self, name, parties):
        self.name = name
        self.parties = parties
        self.generation = 0
        self.r = RedisConn.get_conn()

    def wait(self):
        # Append me to redis list and get size of list
        position = self.r.lpush(self.name + "-list-" +
                                str(self.generation), "")

        # If position in list < parties, Read complete key (block) (gen)
        if (position < self.parties):
            self.r.blpop(self.name + "-complete-" + str(self.generation))
        # Else, I am last, create complete key (gen)
        else:
            s = [str(x) for x in range(self.parties)]
            self.r.lpush(self.name + "-complete-" + str(self.generation), *s)

            # And flush the list (and gen-1 complete)
            self.r.delete(self.name + "-list-" + str(self.generation),
                          self.name + "-complete-" + str(self.generation - 1))

        self.generation += 1
        return position

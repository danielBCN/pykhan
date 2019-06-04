import khan


class RedisBarrierTask(khan.benchmark.Task):

    def do_task(self):
        self.object.wait()

    def new_object(self):
        return khan.synch.red.RedisBarrier("barrier-"+str(self.task_id),
                                           self.parallelism)


if __name__ == "__main__":
    # khan.aws.deploy_handler([__file__])

    redispass = "@4iAgH+WMZ92pgE-"
    khan.benchmark.benchmark(RedisBarrierTask, 1200, 1000, local=False,
                             server='35.175.24.57', redispass=redispass)

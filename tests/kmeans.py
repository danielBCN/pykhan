import boto3
import time
import numpy as np

import khan

THRESHOLD = 0.00001
DATAPOINTS_PER_FILE = 695_866  # dataset 100 dimensions
DIMENSIONS = 100


def main():
    # khan.aws.deploy_handler([__file__])
    # return

    import threading
    local = True

    redispass = None
    # redispass = "@4iAgH+WMZ92pgE-"
    redis_conf = {
        'host': 'localhost',
        # 'host': '35.175.24.57',
        'port': 6379,
        'password': redispass
    }
    khan.synch.red.RedisConn.set_up(**redis_conf)

    parallelism = 1
    clusters = 25
    dimensions = DIMENSIONS
    number_of_iterations = 10

    # TEST Crentroids
    # centroids = GlobalCentroids(2, 2)
    # centroids.random_init(4)
    # print(centroids.get_centroids())
    # centroids.update([[1.2, 1, 1, 1], [2, 2, 2, 2]], [2, 2])
    # centroids.update([[2, 2, 2, 2.2], [1, 1, 1, 1]], [2, 2])
    # print(centroids.get_centroids())
    # return

    # TEST Delta
    # delta = GlobalDelta(2)
    # delta.init()
    # print(delta.get_delta())
    # delta.update(1, 2)
    # delta.update(0, 2)
    # print(delta.get_delta())
    # return

    # Initialize global objects
    centroids = GlobalCentroids(clusters, parallelism)
    centroids.random_init(dimensions)
    delta = GlobalDelta(parallelism)
    delta.init()

    worker_stats = []     # in seconds

    threads = []

    def local_run(w_id):
        worker_breakdown = train(w_id, parallelism * DATAPOINTS_PER_FILE,
                                 dimensions, parallelism, clusters,
                                 number_of_iterations, False, redis_conf)
        worker_stats.append(worker_breakdown)

    def lambda_run(w_id):
        ct = khan.aws.CloudThread(train, w_id, parallelism*DATAPOINTS_PER_FILE,
                                  dimensions, parallelism, clusters,
                                  number_of_iterations, False, redis_conf)
        ct.run()
        worker_breakdown = ct.get_response()
        worker_stats.append(worker_breakdown)

    if local:
        task = local_run
    else:
        task = lambda_run
    threads = [threading.Thread(target=task, args=(i,))
               for i in range(parallelism)]

    start_time = time.time()
    [t.start() for t in threads]
    [t.join() for t in threads]

    # Parse results
    times = []
    for b in worker_stats:
        # Iterations time is second breakdown and last
        times.append(b[-1]-b[2])

    avg_time = sum(times) / len(times)
    print(f"Total k-means time: {time.time() - start_time} s")
    print(f"Average iterations time: {avg_time} s")

    with open('time_break.txt', 'w') as f:
        for item in worker_stats:
            f.write(f"{item}\n")


def train(worker_id, data_points, dimensions, parallelism,
          clusters, max_iters, use_lower_bound_opt, redis_conf):
    khan.synch.red.RedisConn.set_up(**redis_conf)
    worker = Worker(worker_id, data_points, dimensions, parallelism,
                    clusters, max_iters, use_lower_bound_opt)
    return worker.run()  # return time info


class GlobalCentroids(object):
    def __init__(self, clusters, parallelism):
        self.red = khan.synch.red.RedisConn.get_conn()
        self.num_clusters = clusters
        self.parallelism = parallelism

    @staticmethod
    def centroid_key(centroid):
        return "centroid" + str(centroid)

    def random_init(self, num_dimensions):
        import random
        print(f"Initializing GlobalCentroids with {self.num_clusters},"
              f"{num_dimensions},{self.parallelism}")
        for i in range(self.num_clusters):
            random.seed(1002+i)
            numbers = [random.gauss(0, 1) for _ in range(num_dimensions)]
            self.red.delete(self.centroid_key(i))
            self.red.rpush(self.centroid_key(i), *numbers)
            # A counter for the updates
            self.red.set(self.centroid_key(i)+"_c", 0)

    def update(self, coordinates, sizes):
        for k in range(self.num_clusters):
            self._update_centroid(k, coordinates[k].tolist(), int(sizes[k]))

    def _update_centroid(self, cluster_id, coordinates, size):
        # lua_script = """
        #     local centroidKey = KEYS[1]
        #     local counterKey = KEYS[2]
        #     local centroidTemp = KEYS[3]
        #     local sizeTemp = KEYS[4]
        #     local n = redis.call("LLEN", centroidKey)
        #     local count = redis.call("GET", counterKey)
        #     if count == "0" then
        #         redis.call("DEL", centroidTemp)
        #         for i = 0,n-1 do
        #             redis.call("RPUSH", centroidTemp, tostring(0.0))
        #         end
        #         redis.call("SET", sizeTemp, 0)
        #     end
        #     local sum
        #     for i = 0,n-1 do
        #         sum = redis.call("LINDEX", centroidTemp, i)
        #         sum = sum + ARGV[3+i]
        #         redis.call("LSET", centroidTemp, i, tostring(sum))
        #     end
        #     local size = redis.call("INCRBY", sizeTemp, ARGV[1])
        #     count = redis.call("INCR", counterKey)
        #     if tonumber(count) == tonumber(ARGV[2]) then
        #         if size ~= "0" then
        #             redis.call("DEL", centroidKey)
        #             for i = 0,n-1 do
        #                 sum = redis.call("LINDEX", centroidTemp, i)
        #                 sum = sum / size
        #                 redis.call("RPUSH", centroidKey, tostring(sum))
        #             end
        #         end
        #         redis.call("SET", counterKey, 0)
        #     end
        #     return
        # """
        lua_script = """
            local centroidKey = KEYS[1]
            local counterKey = KEYS[2]
            local centroidTemp = KEYS[3]
            local sizeTemp = KEYS[4]
            local n = redis.call("LLEN", centroidKey)
            local count = redis.call("GET", counterKey)
            if count == "0" then
                redis.call("DEL", centroidTemp)
                local a = {}
                for i = 1,2*(n),2 do
                    a[i]   = tostring((i-1)/2)
                    a[i+1] = tostring(0.0)
                end
                redis.call("HMSET", centroidTemp, unpack(a))
                redis.call("SET", sizeTemp, 0)
            end
            for i = 0,n-1 do
                redis.call("HINCRBYFLOAT", centroidTemp, tostring(i), tostring(ARGV[3+i]))
            end
            local size = redis.call("INCRBY", sizeTemp, ARGV[1])
            count = redis.call("INCR", counterKey)
            if tonumber(count) == tonumber(ARGV[2]) then
                if size ~= "0" then
                    redis.call("DEL", centroidKey)
                    local temps = redis.call("HGETALL", centroidTemp)
                    local values = {}
                    for i = 1,n*2,2 do
                        values[(i+1)/2] = temps[i+1]/size
                    end
                    redis.call("RPUSH", centroidKey, unpack(values))
                end
                redis.call("SET", counterKey, 0)
            end
            return
        """
        centroid_k = self.centroid_key(cluster_id)
        coordinates = list(map(lambda x: str(x), coordinates))
        self.red.eval(lua_script, 4, centroid_k, centroid_k+"_c",
                      centroid_k+"_temp", centroid_k+"_st",
                      size, self.parallelism, *coordinates)

    def get_centroids(self):
        b = [self.red.lrange(self.centroid_key(k), 0, -1)
             for k in range(self.num_clusters)]
        return np.array(list(map(lambda point: list(map(lambda v: float(v), point)), b)))


class GlobalDelta(object):
    def __init__(self, parallelism):
        self.red = khan.synch.red.RedisConn.get_conn()
        self.parallelism = parallelism

    def init(self):
        # FIXME Hardcoded keys
        self.red.set("delta", 1)
        self.red.set("delta_c", 0)
        self.red.set("delta_temp", 0)
        self.red.set("delta_st", 0)

    def get_delta(self):
        return float(self.red.get("delta"))

    def update(self, delta, num_points):
        lua_script = """
            local deltaKey = KEYS[1]
            local counterKey = KEYS[2]
            local deltaTemp = KEYS[3]
            local npointsTemp = KEYS[4]

            local tmpDelta = redis.call("INCRBY", deltaTemp, tostring(ARGV[1]))
            local tmpPoints = redis.call("INCRBY", npointsTemp, ARGV[2])

            local count = redis.call("INCR", counterKey)
            if tonumber(count) == tonumber(ARGV[3]) then
                local newDelta = tmpDelta / tmpPoints
                redis.call("SET", deltaKey, newDelta)
                redis.call("SET", counterKey, 0)
                redis.call("SET", deltaTemp, 0)
                redis.call("SET", npointsTemp, 0)
            end
            return
        """
        delta_key = "delta"
        self.red.eval(lua_script, 4, delta_key, delta_key+"_c",
                      delta_key+"_temp", delta_key+"_st",
                      delta, num_points, self.parallelism)


class Worker(object):

    def __init__(self, worker_id, data_points, dimensions, parallelism,
                 clusters, max_iters, use_lower_bound_opt):
        self.worker_id = worker_id
        self.num_dimensions = dimensions
        self.num_clusters = clusters
        self.max_iterations = max_iters
        self.partition_points = int(data_points / parallelism)
        self.parallelism = parallelism
        self.use_lower_bound_opt = use_lower_bound_opt
        self.start_partition = self.partition_points * self.worker_id
        self.end_partition = self.partition_points * (worker_id + 1)

        self.correct_centroids = None
        self.local_partition = None
        self.local_centroids = None
        self.local_sizes = None
        self.local_membership = None

        self.barrier = khan.synch.red.RedisBarrier("barrier", self.parallelism)
        self.global_delta = GlobalDelta(self.parallelism)
        self.global_centroids = GlobalCentroids(self.num_clusters,
                                                self.parallelism)

    def run(self):
        print(f"Thread {self.worker_id}/{self.parallelism} with "
              f"k={self.num_clusters} maxIts={self.max_iterations} "
              f"opt={self.use_lower_bound_opt}")

        self.global_centroids = GlobalCentroids(self.num_clusters,
                                                self.parallelism)
        breakdown = []
        breakdown.append(time.time())

        self.load_dataset()
        print(self.local_partition)

        # self.local_membership = [0 for _ in range(len(self.local_partition))]
        self.local_membership = np.zeros([self.local_partition.shape[0]])

        # barrier before starting iterations, to avoid different execution times
        self.barrier.wait()

        init_time = time.time()
        breakdown.append(init_time)
        iter_count = 0
        global_delta_val = 1
        while (iter_count < self.max_iterations) and (global_delta_val > THRESHOLD):
            print(f"Iteration {iter_count} of worker {self.worker_id}")

            # Get local copy of global objects
            self.correct_centroids = self.global_centroids.get_centroids()
            # if self.use_lower_bound_opt:
            #     self.compute_correct_centroids_norms()
            breakdown.append(time.time())

            # Reset data structures that will be used in this iteration
            # self.local_sizes = [0 for _ in range(self.num_clusters)]
            self.local_sizes = np.zeros([self.num_clusters])
            # self.local_centroids = [[0 for _ in range(self.num_dimensions)]
            #                         for _ in range(self.num_clusters)]
            self.local_centroids = np.zeros(
                [self.num_clusters, self.num_dimensions])
            print("Structures reset")

            # Compute phase, returns number of local membership modifications
            delta = self.compute_clusters()
            breakdown.append(time.time())
            print(f"Compute finished in {breakdown[-1]-breakdown[-2]} s")

            # Update global objects
            self.global_delta.update(delta, self.local_partition.shape[0])
            self.global_centroids.update(
                self.local_centroids, self.local_sizes)

            breakdown.append(time.time())
            p = self.barrier.wait()
            print(f"Await: {p}")
            breakdown.append(time.time())
            global_delta_val = self.global_delta.get_delta()
            print(f"DEBUG: Finished iteration {iter_count} of worker "
                  f"{self.worker_id} [GlobalDeltaVal={global_delta_val}]")
            iter_count += 1
        breakdown.append(time.time())
        iteration_time = breakdown[-1] - init_time
        print(f"{iter_count} iterations in {iteration_time} s")
        return breakdown

    def load_dataset(self):
        import random
        self.local_partition = np.random.randn(self.partition_points,
                                               self.num_dimensions)
        # self.local_partition = S3Reader().get_points(self.worker_id,
        #                                              self.partition_points,
        #                                              self.num_dimensions)

    def compute_correct_centroids_norms(self):
        raise NotImplementedError()  # TODO

    def distance(self, point, centroid):
        """Euclidean squared distance."""
        # distance = 0.0
        # for i in range(self.num_dimensions):
        #     distance += (point[i] - centroid[i]) * (point[i] - centroid[i])
        # return distance
        return np.linalg.norm(point - centroid)

    def find_nearest_cluster(self, point):
        cluster = 0
        min_dis = None
        for k in range(self.num_clusters):
            # if self.use_lower_bound_opt:
            #     # Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this
            #     # lower bound to avoid unnecessary distance computation.
            #     # TODO
            #     pass

            distance = self.distance(self.local_partition[point-self.start_partition],
                                     self.correct_centroids[k])
            if min_dis is None or distance < min_dis:
                min_dis = distance
                cluster = k

        return cluster

    def compute_clusters(self):
        delta = 0

        # for i in range(self.start_partition, self.end_partition):
        for i in range(0, self.end_partition-self.start_partition):
            # cluster = self.find_nearest_cluster(i)
            # Complexity: point*clusters * dims
            point = self.local_partition[i]
            dists = ((point - self.correct_centroids) ** 2).sum(axis=1)
            cluster = np.argmin(dists)

            # For every dimension, add new point to local centroid
            # for j in range(self.num_dimensions):
            #     self.local_centroids[cluster][j] += self.local_partition[i -
            #                                                              self.start_partition][j]
            self.local_centroids[cluster] += point

            self.local_sizes[cluster] += 1

            # If now point is a member of a different cluster
            if self.local_membership[i] != cluster:
                delta += 1
                self.local_membership[i] = cluster

        return delta


class S3Reader(object):
    S3_BUCKET = "gparis-kmeans-dataset"

    def get_points(self, worker_id, partition_points, num_dimensions):
        self.file_name = "dataset-100GB-100d/part-" + "{:05}".format(worker_id)
        print(f"s3BUCKET::::::: {self.S3_BUCKET}")

        s3 = boto3.resource('s3')
        # s3 = boto3.resource('s3', aws_access_key_id='xxx', aws_secret_access_key='xxx')
        obj = s3.Object(self.S3_BUCKET, self.file_name).get()['Body']

        points = np.zeros([partition_points, num_dimensions])

        lines = 0
        for line in obj.iter_lines():
            dims = (line.decode("utf-8")).split(',')
            # points.append(list(map(lambda x: float(x), dims)))
            points[lines] = np.array(list(map(lambda x: float(x), dims)))
            lines += 1

        obj.close()
        print(f"Dataset loaded from file {self.file_name}")
        print(f"First point: {points[0][0]}  "
              f"Last point: {points[partition_points-1][num_dimensions-1]}")
        print(f"Points loaded: {points.shape[0]}")

        for p, point in enumerate(points):
            if len(point) != num_dimensions:
                print(f"Worker {worker_id} Reading ERROR: point {p} "
                      f"only has {len(point)} dimensions!")
        return points


def test_s3():
    s3 = S3Reader()
    s3.get_points(0, DATAPOINTS_PER_FILE, DIMENSIONS)


if __name__ == "__main__":
    main()
    # test_s3()

    # point = [3, 2, 1]
    # clusters = [[1, 2, 3],
    #             [1, 3, 2],
    #             [3, 2, 1]]

    # distances = []
    # for k in clusters:
    #     dist = 0.0
    #     for d in range(3):
    #         dist += (point[d]-k[d]) * (point[d]-k[d])
    #     distances.append(dist)
    # print(distances)

    # point = np.array([3, 2, 1])
    # print(f"Point: {point}")

    # clusters = np.array([[1, 2, 3],
    #                      [1, 3, 2],
    #                      [3, 2, 1]])

    # distances = ((point - clusters)**2).sum(axis=1)

    # print(distances)
    # print(np.argmin(distances))

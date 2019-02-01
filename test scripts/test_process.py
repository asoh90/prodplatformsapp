from multiprocessing import Process
# import test_queue
import threadpool
import time

def test(test_list, i):
    time.sleep(2)
    test_list.append(i)
    print("Test List: {}".format(test_list))

# def queue_test(queue, test_list, i):
#     queue.put(test(test_list, i))

# if __name__ == "__main__":
#     test_list = []
#     processes = []

#     # process = Process(target=queue_test, args=(test_queue.queue, test_list, 1))
#     # process.start()
#     # process.join()

#     for i in range(100):
#         process = Process(target=queue_test, args=(test_queue.queue, test_list, i))
#         process.start()
#         processes.append(process)

#     print("Joining processes")
#     process_joined = 0
#     for p in processes:
#         p.join()
#         process_joined += 1
#         print(process_joined)

#     print("Final A: {}".format(test_list))

test_list = []
pool = threadpool.ThreadPool(5)
for i in range(10):
    pool.add_task(test, test_list, i)

pool.wait_completion()
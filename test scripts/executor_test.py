import executor_manager
# from concurrent.futures import ThreadPoolExecutor
import time

def test(test_list, i):
    time.sleep(10)
    test_list.append(i)
    print("Test List: {}".format(test_list))

executor = executor_manager.executor
test_list = []

for i in range(100):
    a = executor.map(test, test_list, i)

print("Final A: {}".format(test_list))

# from concurrent.futures import ThreadPoolExecutor
# import time
# def wait_on_b():
#     time.sleep(5)
#     print("b: 5")  # b will never complete because it is waiting on a.
#     return 5

# def wait_on_a():
#     time.sleep(5)
#     print("a: 6")  # a will never complete because it is waiting on b.
#     return 6


# executor = ThreadPoolExecutor(max_workers=2)
# a = executor.submit(wait_on_b)
# b = executor.submit(wait_on_a)
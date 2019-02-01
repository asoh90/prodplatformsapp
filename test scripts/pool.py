from multiprocessing import Pool
import time

def test(i):
    time.sleep(10)
    print("Value: {}".format(i))

def test_two(i):
    time.sleep(15)
    print("Value Two: {}".format(i*i))

def main():
    pool = Pool(processes=4)
    # Below getting the pool from another file doesn't work
    # pool = pool_manager.pool_list[0] 

    for i in range(10):
        print("Running Thread: {}".format(i))
        pool.apply_async(test, args=(i,))
        pool.apply_async(test_two, args=(i,))

    pool.close()
    pool.join()

main()
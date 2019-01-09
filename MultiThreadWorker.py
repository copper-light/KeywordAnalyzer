import threading
import multiprocessing
from collections import deque
import datetime
from time import sleep


class MultiThreadWorker:
    def __init__(self, limit=multiprocessing.cpu_count(), func=None, works=None):
        print("start converting work")
        if works is not None:
            self._func = func
            self._deque = deque(works)
            self._result = [None] * len(self._deque)
        print("finish converting")

        self._read_count = 0
        self._done_count = 0
        self._processing_size = 1000
        self._limit = limit
        self._critical_section_lock = threading.Lock()
        self._check_term = 1 # sec

    def process_work(self):
        deq = self._deque

        while len(deq) > 0:
            ret = []
            works = []
            tmp_time = datetime.datetime.now()
            with self._critical_section_lock:
                if len(deq) > self._processing_size:
                    read_len = self._processing_size
                else:
                    read_len = len(deq)

                for i in range(read_len):
                    work = deq.popleft()
                    works.append([self._read_count, work])
                    self._read_count += 1
            print("t- pre", datetime.datetime.now() - tmp_time)

            tmp_time = datetime.datetime.now()
            for work in works:
                ret.append([work[0], self._func(work[0], work[1])])
            print("t- mid", datetime.datetime.now() - tmp_time)

            tmp_time = datetime.datetime.now()
            with self._critical_section_lock:
                if len(ret) > 0 : print(ret[len(ret)-1][0])
                for data in ret:
                    self._result[data[0]] = data[1]
                    self._done_count += 1
            print("t- post", datetime.datetime.now() - tmp_time)

    def setwork(self, func, works):
        self._func = func
        self._deque = deque(works)
        self._done_count = 0
        self._result = [None] * len(self._deque)

    def process_data_size(self, size):
        if size > 0:
            self.processing_size = size

    def setcheckterm(self, sec):
        self._check_term = sec

    def getresult(self):
        return self._result

    def getthreadsize(self):
        return self._limit

    def start_and_wait(self):
        total_count = len(self._deque)
        work_idx = 0
        done_count = 0

        #thread 4개를 만든다
        thread_list = []
        for t_idx in range(self._limit):
            t = threading.Thread(target=self.process_work)
            t.start()
            thread_list.append(t)

        for thread in thread_list:
            thread.join()

        # while total_count > done_count:
        #     remain_work_count = len(self._deque)
        #     if threading.active_count() < self._limit and remain_work_count > 0:
        #         work = self._deque.popleft()
        #         threading.Thread(target=self.process_work, args=(work_idx, work)).start()
        #         work_idx += 1
        #         # print("working thread", threading.active_count(), remain_work_count)
        #     if self._check_term > 0:
        #         sleep(self._check_term)
        #
        #     with self._critical_section_lock:
        #         done_count = self._done_count
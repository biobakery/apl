import os
import sys
import time
import heapq
import signal
import itertools

import falcon
import simplejson

import settings

DEFAULT_MAX_OPEN_FILES = 100
DEFAULT_WORK_DIR = "./"

LOG_SUFFIX = ".log"
LOCKFILE_NAME = "apl.lock"


class FileLRU(dict):
    def __init__(self, max_open=DEFAULT_MAX_OPEN_FILES,
                 workdir=DEFAULT_WORK_DIR, *args, **kwargs):
        self.q        = []
        self.cntr     = itertools.count(0)
        self._workdir = None
        self.max_open = max_open

        self.workdir = workdir
        self._openall()
        super(FileLRU, self).__init__(*args, **kwargs)


    @property
    def workdir(self):
        return self._workdir

    @workdir.setter
    def workdir(self, workdir):
        now = str(int(time.time()))
        ns = itertools.chain([""], itertools.imap(str, itertools.count(0)))
        for n in ns:
            if not os.path.exists(os.path.join(workdir+n, LOCKFILE_NAME)):
                real_workdir = workdir+n
                if not os.path.exists(real_workdir):
                    os.mkdir(real_workdir)
                open(os.path.join(real_workdir, LOCKFILE_NAME), 'w').close()
                break
        timed_workdir = os.path.join(real_workdir, now)
        os.mkdir(timed_workdir)
        self._real_workdir = real_workdir
        self._base_workdir = workdir
        self._workdir = timed_workdir


    def open(self, key):
        fname = os.path.join(self.workdir, key) + LOG_SUFFIX
        entry = self[key] = (next(self.cntr), open(fname, 'a'))
        heapq.heappush(self.q, entry)


    def _openall(self):
        for f in os.listdir(self.workdir):
            if not f.endswith(LOG_SUFFIX):
                continue
            key = os.path.basename(f).rstrip(LOG_SUFFIX)
            self.open(key)
        

    def closeall(self):
        while self.q:
            _, f = heapq.heappop(self.q)
            f.close()
            self.pop(os.path.basename(f.name).rstrip(LOG_SUFFIX))
        os.remove(os.path.join(self._real_workdir, LOCKFILE_NAME))


    def recycle(self):
        prev_workdir = self._real_workdir
        self.closeall()
        self.workdir = prev_workdir
        self._openall()


    def __getitem__(self, key):
        if key not in self:
            self.open(key)
        
        if len(self) >= self.max_open:
            entry = heapq.heappop(self.q)
            self.pop(entry[1].name)
            entry[1].close()

        return super(FileLRU, self).__getitem__(key)[1]



if hasattr(settings, "workdir"):
    file_lru = FileLRU(workdir=settings.workdir)
else:
    file_lru = FileLRU()


def sigint_handler(signal, frame):
    msg = "Caught signal {}. Shutting down...".format(signal)
    print >> sys.stderr, msg
    file_lru.closeall()

def sighup_handler(signal, frame):
    msg = "Caught signal {}. Reopening logs...".format(signal)
    print >> sys.stderr, msg
    file_lru.recycle()
    
signal.signal(signal.SIGINT, sigint_handler)
signal.signal(signal.SIGHUP, sighup_handler)


def task_basename(name):
    return name.split(":")[-2]


def save_perf(hostname, f_like):
    for perf_d in simplejson.load(f_like):
        perf_d['host'] = hostname
        key = task_basename(perf_d['name'])
        print >> file_lru[key], simplejson.dumps(perf_d)
    

class APLHandler(object):
    def on_get(self, req, resp):
        resp.body = simplejson.dumps({
            "len": len(file_lru),
            "smallest": [ (f.name, f.tell()) for _, f
                          in file_lru.q[:10] ],
            "largest": [ (f.name, f.tell()) for _, f
                         in file_lru.q[-10:] ]
        })


    def on_post(self, req, resp):
        if not req.content_type or 'application/json' not in req.content_type:
            raise falcon.HTTPUnsupportedMediaType(
                "Need 'Content-Type: application/json'")
        try:
            save_perf(req.env['REMOTE_ADDR'], req.stream)
        except Exception as e:
            resp.body = str(e)
            resp.status = falcon.HTTP_500
        else:
            resp.body = "thx"
            resp.status = falcon.HTTP_201
        

api = falcon.API()
api.add_route("/api", APLHandler())


import logging
from ec2_purge_snapshots import *

logging.basicConfig()

event = {
    "volumes": ["all"],
    # "volumes": ["vol-5efa71fa", "vol-788568d2"],
    # "volumes": ["vol-1f8b685b"],
    "hours": 2,
    "days": 7,
    "weeks": 4,
    "months": 12,
    "years": 10,
    "time": "2016-10-10T14:00:00Z",
    # "volume_tags": {"backup": "daily"},
    "volume_tags": {},
    "dry_run": False
}

def succeed():
    pass

def fail():
    pass

context = {succeed: succeed, fail: fail}

main(event, context)

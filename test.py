from ec2_purge_snapshots import *

event = {
    "volumes": ["all"],
    # "volumes": ["vol-5efa71fa", "vol-788568d2"],
    # "volumes": ["vol-5efa71fa"],
    "hours": 2,
    "days": 7,
    "weeks": 4,
    "months": 12,
    "time": "2016-07-25T14:00:00Z",
    "volume_tags": {"backup": "daily"},
    "dry_run": True
}
context = {}

main(event, context)

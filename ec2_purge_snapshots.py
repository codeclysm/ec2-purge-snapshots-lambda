from __future__ import print_function
from datetime import datetime, timedelta
from dateutil import parser, relativedelta, tz
from boto3 import resource

def main(event, context):
    event = validate_event(event)

    ec2 = resource("ec2", region_name=event["region"])

    if "volumes" in event:
        if len(event["volumes"]) == event["volumes"].count("all"):
            volumes = ec2.volumes.all()
        else:
            volumes = []
            for volume_id in event["volumes"]:
                volume = ec2.Volume(volume_id)
                volume.describe_status() # Will raise an exception if it's not found
                volumes.append(volume)
    elif "volume_tags" in event:
        pass

    for volume in volumes:
        print(volume)
        purge_snapshots(ec2, volume, event)

def validate_event(event):
    if "volumes" not in event and "volume_tags" not in event:
        raise Exception('event should contain a volumes or volume_tags key')
    if "volume_tags" not in event:
        event["volume_tags"] = {}
    if "region" not in event:
        event["region"] = "us-east-1"
    if "dry_run" not in event:
        event["dry_run"] = True
    if "timezone" not in event:
        event["timezone"] = "UTC"
    if "time" not in event:
        event["time"] = datetime.now(tz.gettz(event["timezone"]))
    else:
        event["time"] = parser.parse(event['time']).astimezone(tz.gettz(event["timezone"]))
    if "hours" not in event:
        event["hours"] = 0
    if "days" not in event:
        event["days"] = 0
    if "weeks" not in event:
        event["weeks"] = 0
    if "months" not in event:
        event["months"] = 0
    return event

def purge_snapshots(ec2, volume, event):
    snaps = get_snapshots(ec2, volume, event["volume_tags"])

    hours_threshold = event["time"] - relativedelta.relativedelta(hours=event["hours"])
    days_threshold = event["time"] - relativedelta.relativedelta(days=event["days"])
    weeks_threshold = event["time"] - relativedelta.relativedelta(weeks=event["weeks"])
    months_threshold = event["time"] - relativedelta.relativedelta(months=event["months"])

    ## Uncomment to test
    # event["dry_run"] = True
    # class Snap(dict):
    #     def __getattr__(self, name):
    #         return self[name]
    #
    # snaps = [
    #     Snap(snapshot_id='keep 6 months ago', parser.parse(start_time='2016-01-02T14:00:00Z'),
    #     Snap(snapshot_id='delete 6 months ago (duplicate)', parser.parse(start_time='2016-01-15T14:00:00Z'),
    #     Snap(snapshot_id='keep 5 months ago', parser.parse(start_time='2016-02-15T14:00:00Z'),
    #     Snap(snapshot_id='keep 4 weeks ago', parser.parse(start_time='2016-06-27T14:00:00Z'),
    #     Snap(snapshot_id='delete 4 weeks ago (duplicate)', parser.parse(start_time='2016-06-28T14:00:00Z'),
    #     Snap(snapshot_id='keep 7 days ago', parser.parse(start_time='2016-07-18T14:00:00Z'),
    #     Snap(snapshot_id='delete 7 days ago (duplicate)', parser.parse(start_time='2016-07-18T14:00:00Z'),
    #     Snap(snapshot_id='keep 6 days ago', parser.parse(start_time='2016-07-19T14:00:00Z'),
    #     Snap(snapshot_id='delete 6 days ago (duplicate)', parser.parse(start_time='2016-07-19T14:00:00Z'),
    #     Snap(snapshot_id='keep today', parser.parse(start_time='2016-07-25T06:00:00Z'),
    #     Snap(snapshot_id='delete today (duplicate)', parser.parse(start_time='2016-07-25T06:00:00Z'),
    #     Snap(snapshot_id='keep 1 hour ago', parser.parse(start_time='2016-07-25T13:00:00Z'),
    #     Snap(snapshot_id='keep 1 hour ago (duplicate)', parser.parse(start_time='2016-07-25T13:30:00Z'),
    #     Snap(snapshot_id='keep latest', parser.parse(start_time='2016-07-25T13:30:00Z'),
    # ]
    # for snap in snaps:
    #     snap.start_time = parser.parse(snap.start_time).astimezone(tz.gettz(event["timezone"]))
    ## End Uncomment to test

    newest = snaps[-1]

    kept = []

    for snap in snaps:
        snap_date = snap.start_time.astimezone(tz.gettz(event["timezone"]))
        snap_age = relativedelta.relativedelta(event["time"], snap_date)
        # Always keep the last snapshot
        if snap.snapshot_id == newest.snapshot_id:
            print(("- Keeping {}: {}, {} hours old - will never"
                  " delete newest snapshot").format(
                  snap.snapshot_id, snap_date,
                  snap_age.seconds/3600)
                  )
            continue
        # Keep snapshots younger than hour threshold
        if snap_date > hours_threshold:
            print("- Keeping {}: {}, {} hours old - {}-hour threshold".format(
                  snap.snapshot_id, snap_date, snap_age.seconds/3600, event["hours"])
                  )
            continue
        # Keep a snapshot per day until the days_threshold
        elif snap_date > days_threshold:
            first_day_str = snap_date.strftime("%Y-%m-%d")
            first_day = parser.parse(first_day_str)
            if first_day_str not in kept:
                kept.append(first_day_str)
                print("- Keeping {}: {}, {} days old - day of {}".format(
                      snap.snapshot_id, snap_date, snap_age.days, first_day_str)
                      )
                continue
        # Keep a snapshot per week until the week_threshold
        elif snap_date > weeks_threshold:
            week_day = int(snap_date.strftime("%w"))
            first_day = snap_date - timedelta(days=week_day)
            first_day_str = first_day.strftime("%Y-%m-%d")
            if first_day_str not in kept:
                kept.append(first_day_str)
                print("- Keeping {}: {}, {} days old - day of {}".format(
                      snap.snapshot_id, snap_date, snap_age.days, first_day_str)
                      )
                continue
        # Keep a snapshot per month until the month_threshold
        elif snap_date > months_threshold:
            first_day = datetime(snap_date.year, snap_date.month, 1)
            first_day_str = first_day.strftime("%Y-%m-%d")
            if first_day_str not in kept:
                kept.append(first_day_str)
                print("- Keeping {}: {}, {} months old - month of {}".format(
                      snap.snapshot_id, snap_date, snap_age.months, first_day_str)
                      )
                continue

        # Delete snapshot
        if event["dry_run"]:
            not_really = " (not really)"
        else:
            not_really = ""
            snap.delete()

        print("--- Deleting{} {}: {}".format(not_really, snap.snapshot_id, snap_date))

def get_snapshots(ec2, volume, volume_tags):
    collection_filter = [
        {
            "Name": "volume-id",
            "Values": [volume.id]
        },
        {
            "Name": "status",
            "Values": ["completed"]
        }
    ]

    for key, value in volume_tags.iteritems():
        collection_filter.append(
            {
                "Name": "tag:" + key,
                "Values": [value]
            }
        )

    collection = ec2.snapshots.filter(Filters=collection_filter)
    return sorted(collection, key=lambda x: x.start_time)

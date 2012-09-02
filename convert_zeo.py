""" convertzeo - Converts myZeo export files into a simple log file

Python script for converting one or more files in the myZeo CSV format into a
log line that's easily parsed by utilities such as Splunk.

"""

__author__ = "Ed Hunsinger"
__copyright__ = "Copyright 2012"
__email__ = "edrabbit@edrabbit.com"

import csv
import datetime
import glob
import pytz
import sys

states = {"0": "undefined",
          "1": "wake",
          "2": "REM",
          "3": "Light",
          "4": "Deep"}

datetime_fields = ("Start of Night", "End of Night", "Rise Time",
                   "First Alarm Ring", "Last Alarm Ring", "First Snooze Time",
                   "Last Snooze Time", "Set Alarm Time")

def add_tzinfo(dt, tz):
    return dt.replace(tzinfo=pytz.timezone(tz))

def convert_zeo_to_log(file, output_file=None, tz=None):
    if not tz:
        tz = "UTC"
    zeo_reader = csv.DictReader(open('zeodata.csv', 'rb'), delimiter=",")
    all_events = []
    for z in zeo_reader:
        # Parse the detailed sleep graph (30 second intervals)
        # TODO(ed): Make this optional as it might not be useful to everyone
        detailed_sleep_graph = z["Detailed Sleep Graph"]
        time_track = datetime.datetime.strptime(z["Start of Night"],
            '%m/%d/%Y %H:%M')
        time_track = add_tzinfo(time_track, tz)
        # for each value in detailed sleep graph
        for state in detailed_sleep_graph.split(" "):
            all_events.append("%s state=%s\n" % (time_track.isoformat(),
                                               states[state]))
            # add 30 seconds to time tracker
            time_track = time_track + datetime.timedelta(seconds=30)

        # Get the summary line
        event = []
        for i in z.iterkeys():
            if i in datetime_fields:
                if z[i]:
                    converted_date = add_tzinfo(
                        datetime.datetime.strptime(z[i], '%m/%d/%Y %H:%M'), tz)
                    z[i] = converted_date.isoformat()
            event.append('%s="%s"' % (i.replace(" ", "_"), z[i]))
        # log_event is a key=value list, we want a key=vaue comma-separated string
        event.sort()
        summary_time = z["End of Night"]
        log_line = "%s %s\n" % (summary_time, ", ".join(event))
        all_events.append(log_line)

    # Output to file
    # TODO(ed): Handle creation/appending files better.
    outfile = open(output_file, "a")

    for event in all_events:
        print event
        outfile.write(event)
    outfile.close()
    print "Output written to %s" % output_file

if __name__ == "__main__":
    print "Processing zeo files..."
    if len(sys.argv) < 3:
        print 'Usage: %s [zeo_files] [timezone] [output_file]' % sys.argv[0]
        print 'Wild cards are permitted in [zeo_files]'
        print 'Timezone should be pytz compatible string, i.e "US/Eastern"'
        print ('If you specify wildcards in zeo_files and provide an '
               'output_file, everything will be combined into one output_file')
        exit(1)
    else:
        list_of_files = glob.glob(sys.argv[1])
        # Sort files by filenames, assuming some sort of datebased filename
        list_of_files.sort()

        tzinfo = sys.argv[2]
        output_file = sys.argv[3]

        for file in list_of_files:
            convert_zeo_to_log(file, output_file, tzinfo)
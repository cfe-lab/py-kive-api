from kiveapi.kiveapi import KiveAPI
import sched
import time

# This is how I would recommend authenticating to Kive
KiveAPI.AUTH_TOKEN = 'b736b807115369b22221ad1e893f26132ed03e19'
KiveAPI.SERVER_URL = 'http://127.0.0.1:8000/'

kive = KiveAPI()

# Upload data
fastq1 = kive.add_dataset('New fastq file 1', 'None', open('exfastq1.fastq', 'r'))
fastq2 = kive.add_dataset('New fastq file 2', 'None', open('exfastq2.fastq', 'r'))

# Get the pipeline by family ID
pipeline_family = kive.get_pipeline_family(2)

print 'Using data:'
print fastq1, fastq2

print 'With pipeline:'
print pipeline_family.published_or_latest()

# Run the pipeline
status = kive.run_pipeline(
    pipeline_family.published_or_latest(),
    [fastq1, fastq2]
)

# Start polling Kive
s = sched.scheduler(time.time, time.sleep)
def check_run(sc, run):
    # do your stuff
    print run.get_status()

    if run.is_running() or run.is_complete():
        print run.get_progress(), run.get_progress_percent(), '%'

    if not run.is_complete():
        sc.enter(5, 1, check_run, (sc, run,))

s.enter(5, 1, check_run, (s, status,))
s.run()

print 'Finished Run, nabbing files'
for dataset in status.get_results():
    file_handle = open(dataset.filename, 'w')
    dataset.download(file_handle)
    file_handle.close()
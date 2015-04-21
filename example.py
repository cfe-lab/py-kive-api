from kiveapi.kiveapi import KiveAPI
import sched
import time


URL = 'http://127.0.0.1:8000/'
KiveAPI.AUTH_TOKEN = KiveAPI.get_token(URL, 'kive', 'kive')
KiveAPI.SERVER_URL = URL

print KiveAPI.AUTH_TOKEN
# I recommend not using get_token to retrieve the auth token
# (see example_2.py). Putting a password in a distributed file
# is not recommended. An auth token, however, can easily be
# revoked, so that should be the preferred method of authentication

kive = KiveAPI()

# Get the data by ID
fastq1 = kive.get_dataset(2)
fastq2 = kive.get_dataset(3)

# or get the data by name
fastq1 = kive.find_datasets(dataset_name='1234A_R1.fastq')[0]
fastq2 = kive.find_datasets(dataset_name='1234A_R2.fastq')[0]

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
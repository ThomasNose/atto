# Startup
(windows)
You should create a virtual environment as follows
>python -m venv .venv
>.venv/Scripts/activate.bat
>pip install -r requirements.txt

You also need java 17 jdk in order to use spark correctly.

You can run "pytest" locally to test once all modules are installed.
You can see the result of transform.py by running it directly. Indirectly running it, from another file, will not yield visual results.

Any issues beyond this point will generally be environment related and may require you to point the correct java/python versions into your venv though I'm hoping this isn't a problem.

# Main scripts

### data_prep.transform.py
### tests.test_transforms
### tests.conftest.py
### artifacts.request_model.py

# Documentation and thought processes

## Challenges
The part of this exercise I most struggled with was the fastapi/prediction as I've not had exposure to creating APIs before. Although my I've interacted with them in the past, I've never been faced with a challenge to create an API myself.

I ended up using claude a lot when troubleshooting fastapi and getting it to work locally was a bit of a pain as I didn't know all the right commands/packages required to begin with. There were also problems with my function to write CSVs using pyspark due to problems with hadoop that I didn't bother investigating for this exercise.

## Tradeoffs
I decided to use pyspark for this exercise as the task itself implied grow of 12% per quarter which is exponential in nature. Pyspark as a tool therefore allows us to distribute our data away from a driver into multiple workers and allows for more control over optimisation.

For example, in transform.py I used spark.conf.set("spark.sql.shuffle.partitions", "1") because the dataset I'm working with locally is incredibly small and anymore than this would just be task queue hell.

## Production
This isn't production ready as there's clearly bugs/problems with my predictions, at least on the surface, and so I'd need to consult with others as to mistakes I've made.

In terms of actual infrastructure, I can't see a reason why the app and transformation code wouldn't work on a budget of 500 dollars per month. A virtual machine, specifically the A4 8vCPU 14GB ram, costs just shy of 400 dollars per month providing both multiple cores for us to expose our app to. We could however instead opt for five A2 2vCPU 3.5GB RAM machines which, at a cost of 98 dollars each, would give us a dedicated machine for our app, a driver for our pyspark job, and upwards of three worker machines for our distributed workloads.

As for scalability, possibly at the start depending on initial loads, we may not need to scale to five total VMs and thus that room of three VMs allows us to deploy multiple pods for our APIs or leverage more spark distribution in future.

As for provements or changes, I'd probably make changes to the API as there's zero protection again DDOS (assuming public ip) or any other security measures. The model itself too may be too basic as I had multiple parameters available but couldn't pass them all in.

## fastapi

I personally wouldn't know the best way to deploy this. I know of pods which are used to distribute workloads and provide disaster recovery using kubernetes, though I'm not sure whether that's appropriate in this case. 

## data volume

If transactions were to jump to millions per day then the only thing I'd change is my shuffle partitioning and VM infrastructure. I used pyspark for a reason and it's due to it's easy scalability with large data volumes. You can get really into spark optimisation but it's rare you really need to get that deep into it unless your data volumes/machine learning grows into a monster...

## Metrics

I'd track batch size and time to complete for any transformation work as that'll give us an indicator if anything is struggling. As for the API, you'd want to track
- request rate per second/minute/15 minutes etc
- response code amounts within certain windows (this helps identify problems with the API like data quality or API bugs)
- API response time
If the data isn't funneled down some kind of schema registry then you could get multiple providers sending data that are slightly different that'll cause problems when posting on a large scale. The model itself also doesn't seem to care about time-scale or frequency so much so a person self-employed may earn a large amount of money for 6-9 months of the year but have no payroll information for months. The model therefore could fire false positives when trying to fit different demographics.

## AI tooling

I primarily used claude during this exercise as I have had no experience with creating APIs; since I got this task I hadn't even heard of fastapi.

There was also a point in time when my unit testing was failing and I printed the schemas of my two dataframes to quickly find the differences which allowed me to then correct my Transform class.

A specific command that claude helped me with was 
> concat_ws(",", array_sort(collect_set("description_category"))).alias("categories")

as it's a bit of a convoluted command to collect all strings into one line in an aggregation and I don't often do it. I essentially cheated to get this bit done quicker. However, it was wrong as the elements weren't in the correct alphabetical order like I wanted so I applied array_sort.


## Extra info/DQ problems
labels seems to have no major problems that I can see, customer_id is prefixed CUST with incrementing integer values although the space here only has 0000 number suffix meaning we can only have 9999 customers?
This is an assumption that the number won't just default to CUST-10000 and CUST-10001. The customer_id however has no inherent problems if we wanted to cluster/z-order etc.

transactions also doesn't have too many problems though, similar to labels, has a problem with customer_id and transaction_id T99999 possibly being the highest ID achievable.
I also notice that the amount field is a decimal 2/double which could cause floating point errors further downstream resulting in weird reconciliation. I'd suggest keeping this value in pence until the final analysis step.
I'm assuming description has a hard limit upstream of 255-1024 characters though if stored as a string it's size will vary depending on size, at least in postgresql.
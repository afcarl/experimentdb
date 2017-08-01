# this shows how the simplex db should be used

from __future__ import print_function
import experimentdb as edb
import random
import timeit

# open (or create) a database
db = edb.open('cremi')

# start from a fresh database for demonstration purposes
db.clear()

print("Storing 100.000 results...")
# store something
for i in range(1,100000):
    db.put(
            edb.Key(setup='setup91', iteration=i),
            edb.Value(loss=random.random())
    )

print("Storing result with different key")
# store something else
db.put(
        edb.Key(setup='setup91', iteration=400000, sample='sample_C', merge_function='median_aff', threshold=0.82),
        edb.Value(splits=12, merges=23)
)

print("Updating a result")
# update a value (should fail)
try:
    db.put(
            edb.Key(setup='setup91', iteration=400000, sample='sample_C', merge_function='median_aff', threshold=0.82),
            edb.Value(splits=12, merges=24)
    )
except RuntimeError as e:
    print("Received:", e)
# update a value
db.put(
        edb.Key(setup='setup91', iteration=400000, sample='sample_C', merge_function='median_aff', threshold=0.82),
        edb.Value(splits=12, merges=24),
        allow_update=True
)

# get a pandas dataframe with all results
print("Creating dataframe...")
df = db.get()

print("All results:")
print(df)
print()

# get only selected results
print("Creating dataframe for selected results...")
df = db.get(edb.Key(iteration=400000))

print("All results at iteration 400000:")
print(df)
print()

# we added a new parameter 'init_with_max' and have new results
print("Add a result with a new subkey")
db.put(
        edb.Key(setup='setup91', iteration=400000, sample='sample_C', merge_function='median_aff', threshold=0.82, init_with_max=True),
        edb.Value(splits=10, merges=21)
)

print("Creating dataframe for selected results...")
df = db.get(edb.Key(iteration=400000))

print("All results at iteration 400000:")
print(df)
print()

# to correctly tag previous results, back-fill 'init_with_max'
print("Backfilling new subkey for previous results...")
db.backfill(
        subkey='init_with_max',
        value=False,
        limit_to=edb.Key(merge_function='median_aff') # a partial key
)

df = db.get(edb.Key(iteration=400000))
print("All results at iteration 400000:")
print(df)
print()

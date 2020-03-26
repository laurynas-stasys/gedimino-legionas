import pandas as pd

with open('/Users/laurynas/places.txt', 'r') as in_str:
    lines = in_str.readlines()

j = 0
values = []
while j < len(lines):

    chunk = lines[j:j+4]

    ll = {}
    for chunk_line in [_.strip().split(':') for _ in chunk[1:3]]:
        ll[chunk_line[0].split('\"')[1]] = chunk_line[1].split('\"')[1]

    values.append(ll)

    j += 4

df = pd.DataFrame.from_records(values)
df.columns = ['adresas', 'vieta']
df.to_csv('lokacijos.csv')
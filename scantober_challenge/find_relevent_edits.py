import csv

cat_dates = [
    {'category': 'en:camomile-teas', 'date': "2026-10-01"},
    {'category': 'en:breads-with-seeds', 'date': "2026-10-02"},
    {'category': 'en:plain-cow-milk-yogurts', 'date': "2026-10-03"},
    {'category': 'en:squid', 'date': "2026-10-04"},
    {'category': 'en:stews', 'date': "2026-10-05"},
    {'category': 'en:orange-juices', 'date': "2026-10-06"},
    {'category': 'en:margarines', 'date': "2026-10-07"},
    {'category': 'en:bacon', 'date': "2026-10-08"},
    {'category': 'en:light-beers', 'date': "2026-10-09"},
    {'category': 'en:vegetable-salads', 'date': "2026-10-10"},
    {'category': 'en:jams', 'date': "2026-10-11"},
    {'category': 'en:bean-dishes', 'date': "2026-10-12"},
    {'category': 'en:chicken-and-vegetables-soup', 'date': "2026-10-13"},
    {'category': 'en:frying-oil', 'date': "2026-10-14"},
    {'category': 'en:pickled-fruits', 'date': "2026-10-15"},
    {'category': 'en:lemon-juice', 'date': "2026-10-16"},
    {'category': 'en:meat-balls', 'date': "2026-10-17"},
    {'category': 'en:canned-diced-mixed-vegetables', 'date': "2026-10-18"},
    {'category': 'en:hamburger-buns', 'date': "2026-10-19"},
    {'category': 'en:tomato-juices', 'date': "2026-10-20"},
    {'category': 'en:mint-teas', 'date': "2026-10-21"},
    {'category': 'en:liqueurs', 'date': "2026-10-22"},
    {'category': 'en:whole-wheat-english-muffins', 'date': "2026-10-23"},
    {'category': 'en:mix-of-salad-leaves', 'date': "2026-10-24"},
    {'category': 'en:egg-sandwiches', 'date': "2026-10-25"},
    {'category': 'en:stuffed-olives', 'date': "2026-10-26"},
    {'category': 'en:offals-soup', 'date': "2026-10-27"},
    {'category': 'en:fermented-teas', 'date': "2026-10-28"},
    {'category': 'en:vegetable-bouillon-powders', 'date': "2026-10-29"},
    {'category': 'en:marinated-fish', 'date': "2026-10-30"},
    {'category': 'en:pizzas', 'date': "2026-10-31"},
]

f = open('scantober_edits.csv', 'r')
reader = csv.reader(f)
data = []
for row in reader:
    data.append(row)

perfect_matches_count = 0
likely_matches_count = 0
perfect_matches_list = []
for cat_date in cat_dates:
    for row in data:
        if cat_date['category'] in row[1]:
            if cat_date['date'] == row[2]:
                print(f'{row[0]} {row[1]} {row[2]} {row[3]} - perfect match')
                perfect_matches_count += 1
                perfect_matches_list.append(row)
            if cat_date['date'] < row[2]:
                likely_matches_count += 1

print(f'perfect_matches_count : {perfect_matches_count}')
print(f'likely_matches_count : {likely_matches_count}')

output = open('edits_on_correct_day.csv', 'w')
writer = csv.writer(output)
writer.writerows(perfect_matches_list)

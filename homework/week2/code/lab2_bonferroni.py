import random
from collections import defaultdict

random.seed(42)  # de ket qua tai lap duoc cho bao cao

N_PEOPLE, N_HOTELS, N_DAYS, P_VISIT = 2000, 50, 30, 0.3   # thu nho tu 10^9

by_hotel_day = defaultdict(list) # (hotel,day) -> [nguoi]
for day in range(N_DAYS):
    for person in range(N_PEOPLE):
        if random.random() < P_VISIT:
            hotel = random.randrange(N_HOTELS)
            by_hotel_day[(hotel, day)].append(person)

pair_days = defaultdict(set) # cap nguoi -> tap ngay da gap nhau
for (hotel, day), people in by_hotel_day.items():
    for i in range(len(people)):
        for j in range(i + 1, len(people)):
            pair_days[tuple(sorted((people[i], people[j])))].add(day)

suspects = sum(1 for days in pair_days.values() if len(days) >= 2)
print(f"So cap 'kha nghi' (du lieu hoan toan ngau nhien): {suspects}")

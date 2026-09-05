gio_hang = [
    {"Bim", "Bia", "Sua"},
    {"Bim", "Bia"},
    {"Bim", "Bia", "Banh"},
    {"Sua", "Banh"},
]

def support(itemset, baskets):
    count = sum(1 for b in baskets if itemset.issubset(b))
    return count / len(baskets)

def confidence(A, B, baskets):
    return support(A | B, baskets) / support(A, baskets)

A = {"Bim"}; B = {"Bia"}
print(f"support(Bim => Bia)    = {support(A|B, gio_hang):.2f}")
print(f"confidence(Bim => Bia) = {confidence(A, B, gio_hang):.2f}")

# Mo rong theo goi y cua slide: thu them luat Banh => Sua
A2 = {"Banh"}; B2 = {"Sua"}
print(f"support(Banh => Sua)    = {support(A2|B2, gio_hang):.2f}")
print(f"confidence(Banh => Sua) = {confidence(A2, B2, gio_hang):.2f}")
